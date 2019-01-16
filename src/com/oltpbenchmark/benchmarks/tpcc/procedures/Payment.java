/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.meetup.memcached.COException;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.procedures.PrettyPrintingMap;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustCDataResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustomerById;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustomerByNameResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetDist2Result;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetWhseResult;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.QueryResult;

public class Payment extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(Payment.class);

    public SQLStmt payUpdateWhseSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET W_YTD = W_YTD + ?  WHERE W_ID = ? ");
    public SQLStmt payGetWhseSQL = new SQLStmt("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" + " FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ?");
    public SQLStmt payUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?");
    public SQLStmt payGetDistSQL = new SQLStmt("SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" + " FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?");
    public SQLStmt payGetCustSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE " + "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payGetCustCdataSQL = new SQLStmt("SELECT C_DATA FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ?, C_DATA = ? " + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payUpdateCustBalSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payInsertHistSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " + " VALUES (?,?,?,?,?,?,?,?)");
    public SQLStmt customerByNameSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " " + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST");

    // Payment Txn
    public PreparedStatement payUpdateWhse = null;//
    public PreparedStatement payGetWhse = null;
    public PreparedStatement payUpdateDist = null;//
    public PreparedStatement payGetDist = null;
    public PreparedStatement payGetCust = null;
    public PreparedStatement payGetCustCdata = null;
    public PreparedStatement payUpdateCustBalCdata = null;//
    public PreparedStatement payUpdateCustBal = null;//
    public PreparedStatement payInsertHist = null;//
    public PreparedStatement customerByName = null;
    
    private final static Logger logger = Logger.getLogger(Payment.class);

    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses, 
            int terminalDistrictLowerID, int terminalDistrictUpperID, Map<String, Object> tres) throws SQLException {
        // initializing all prepared statements
        payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL);
        payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL);
        payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL);
        payGetDist = this.getPreparedStatement(conn, payGetDistSQL);
        payGetCust = this.getPreparedStatement(conn, payGetCustSQL);
        payGetCustCdata = this.getPreparedStatement(conn, payGetCustCdataSQL);
        payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL);
        payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL);
        payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL);
        customerByName = this.getPreparedStatement(conn, customerByNameSQL);

        // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        
        if (TPCCConfig.crossWarehouse) {
            if (x <= 85) {
                customerDistrictID = districtID;
                customerWarehouseID = terminalWarehouseID;
            } else {
                customerDistrictID = TPCCUtil.randomNumber(1, jTPCCConfig.configDistPerWhse, gen);
                do {
                    customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
                } while (customerWarehouseID == terminalWarehouseID && numWarehouses > 1);
            }
        } else {
            customerDistrictID = districtID;
            customerWarehouseID = terminalWarehouseID;
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        boolean customerByName;
        String customerLastName = null;
        customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
//            // 40% lookups by customer ID
            customerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);
        
        if (Config.DEBUG) {
            out.println(String.format("Payment w_id=%s, c_w_id=%s, h_amount=%s, d_id=%s, c_d_id=%s, c_id=%s,"
                    + "c_last=%s, c_by_name=%s, cafe=%b", terminalWarehouseID, customerWarehouseID, paymentAmount, 
                    districtID, customerDistrictID, customerID, customerLastName, customerByName, false));
        }
        
        paymentTransaction(terminalWarehouseID, customerWarehouseID, paymentAmount, 
                districtID, customerDistrictID, customerID, customerLastName, customerByName, 
                conn, tres);

        return null;
    }

    private void paymentTransaction(int w_id, int c_w_id, float h_amount, 
            int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name, 
            Connection conn, Map<String, Object> tres) throws SQLException {
        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;
        if (TPCCConstants.DML_Trace) {
            tres.put("count", 0);
        }

        payUpdateWhse.setFloat(1, h_amount);
        payUpdateWhse.setInt(2, w_id);
        // MySQL reports deadlocks due to lock upgrades:
        // t1: read w_id = x; t2: update w_id = x; t1 update w_id = x
        int result = payUpdateWhse.executeUpdate();
        if (result == 0)
            throw new RuntimeException("W_ID=" + w_id + " not found!");

        if (TPCCConstants.DML_Trace) {
            int count = (Integer) tres.get("count");
            tres.put("DML" + count, payUpdateWhse.toString());
            count++;
            tres.put("count", count);
        }

        payGetWhse.setInt(1, w_id);
        ResultSet rs = payGetWhse.executeQuery();
        if (!rs.next())
            throw new RuntimeException("W_ID=" + w_id + " not found!");
        w_street_1 = rs.getString("W_STREET_1");
        w_street_2 = rs.getString("W_STREET_2");
        w_city = rs.getString("W_CITY");
        w_state = rs.getString("W_STATE");
        w_zip = rs.getString("W_ZIP");
        w_name = rs.getString("W_NAME");
        rs.close();
        rs = null;

        payUpdateDist.setFloat(1, h_amount);
        payUpdateDist.setInt(2, w_id);
        payUpdateDist.setInt(3, d_id);
        result = payUpdateDist.executeUpdate();
        if (result == 0)
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");

        if (TPCCConstants.DML_Trace) {
            int count = (Integer) tres.get("count");
            tres.put("DML" + count, payUpdateDist.toString());
            count++;
            tres.put("count", count);
        }

        payGetDist.setInt(1, w_id);
        payGetDist.setInt(2, d_id);
        rs = payGetDist.executeQuery();
        if (!rs.next())
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
        d_street_1 = rs.getString("D_STREET_1");
        d_street_2 = rs.getString("D_STREET_2");
        d_city = rs.getString("D_CITY");
        d_state = rs.getString("D_STATE");
        d_zip = rs.getString("D_ZIP");
        d_name = rs.getString("D_NAME");
        rs.close();
        rs = null;

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            c = getCustomerByName(c_w_id, c_d_id, c_last);
        } else {
            assert c_last == null;
            c = getCustomerById(c_w_id, c_d_id, c_id, conn);
        }

        String readBalance = TPCCConstants.DECIMAL_FORMAT.format(c.c_balance);
        float readYTD_payment = c.c_ytd_payment;
        int readPayment_cnt = c.c_payment_cnt;
        c.c_balance = c.c_balance - h_amount;
        c.c_ytd_payment += h_amount;
        c.c_payment_cnt += 1;
        String c_data = null;
        String c_balance = TPCCConstants.DECIMAL_FORMAT.format(c.c_balance);
        BigDecimal bd = new BigDecimal(c_balance);
        if (c.c_credit.equals("BC")) { // bad credit

            payGetCustCdata.setInt(1, c_w_id);
            payGetCustCdata.setInt(2, c_d_id);
            payGetCustCdata.setInt(3, c.c_id);
            rs = payGetCustCdata.executeQuery();
            if (!rs.next())
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");
            c_data = rs.getString("C_DATA");
            rs.close();
            rs = null;

            c_data = c.c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id + " " + h_amount + " | " + c_data;
            if (c_data.length() > 500)
                c_data = c_data.substring(0, 500);

            payUpdateCustBalCdata.setBigDecimal(1, bd);
            payUpdateCustBalCdata.setFloat(2, c.c_ytd_payment);
            payUpdateCustBalCdata.setInt(3, c.c_payment_cnt);
            payUpdateCustBalCdata.setString(4, c_data);
            payUpdateCustBalCdata.setInt(5, c_w_id);
            payUpdateCustBalCdata.setInt(6, c_d_id);
            payUpdateCustBalCdata.setInt(7, c.c_id);
            result = payUpdateCustBalCdata.executeUpdate();

            if (result == 0)
                throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id);

            if (TPCCConstants.DML_Trace) {
                int count = (Integer) tres.get("count");
                tres.put("DML" + count, payUpdateCustBalCdata.toString());
                count++;
                tres.put("count", count);
            }

        } else { // GoodCredit

            payUpdateCustBal.setBigDecimal(1, bd);
            payUpdateCustBal.setFloat(2, c.c_ytd_payment);
            payUpdateCustBal.setInt(3, c.c_payment_cnt);
            payUpdateCustBal.setInt(4, c_w_id);
            payUpdateCustBal.setInt(5, c_d_id);
            payUpdateCustBal.setInt(6, c.c_id);
            result = payUpdateCustBal.executeUpdate();

            if (result == 0)
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");

            if (TPCCConstants.DML_Trace) {
                int count = (Integer) tres.get("count");
                tres.put("DML" + count, payUpdateCustBal.toString());
                count++;
                tres.put("count", count);
            }
        }

        if (w_name.length() > 10)
            w_name = w_name.substring(0, 10);
        if (d_name.length() > 10)
            d_name = d_name.substring(0, 10);
        String h_data = w_name + "    " + d_name;

        payInsertHist.setInt(1, c_d_id);
        payInsertHist.setInt(2, c_w_id);
        payInsertHist.setInt(3, c.c_id);
        payInsertHist.setInt(4, d_id);
        payInsertHist.setInt(5, w_id);
        payInsertHist.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
        payInsertHist.setFloat(7, h_amount);
        payInsertHist.setString(8, h_data);
        payInsertHist.executeUpdate();

        if (TPCCConstants.DML_Trace) {
            int count = (Integer) tres.get("count");
            tres.put("DML" + count, payInsertHist.toString());
            count++;
            tres.put("count", count);
        }

        conn.commit();

        if (Config.ENABLE_LOGGING) {
            if (c.c_credit.equals("BC")) {
                tres.put("c_data", c_data);

            }
            tres.put("w_id", c_w_id);
            tres.put("d_id", c_d_id);
            tres.put("c_id", c.c_id);
            tres.put("c_balance", c_balance);
            tres.put("c_ytd_payment", c.c_ytd_payment);
            tres.put("c_payment_cnt", c.c_payment_cnt);

            tres.put("readBalance", readBalance);
            tres.put("readYTD_payment", readYTD_payment);
            tres.put("readPayment_cnt", readPayment_cnt);
        }
        
        if (Config.DEBUG) {
            System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
        }
        
        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+---------------------------- PAYMENT ----------------------------+");
        terminalMessage.append("\n Date: " + TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(w_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(w_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(w_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(w_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(w_zip);
        terminalMessage.append("\n\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(d_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(d_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(d_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(d_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(d_zip);
        terminalMessage.append("\n\n Customer:  ");
        terminalMessage.append(c.c_id);
        terminalMessage.append("\n   Name:    ");
        terminalMessage.append(c.c_first);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_middle);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_last);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(c.c_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(c.c_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(c.c_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(c.c_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(c.c_zip);
        terminalMessage.append("\n   Since:   ");
        if (c.c_since != null) {
            terminalMessage.append(c.c_since.toString());
        } else {
            terminalMessage.append("");
        }
        terminalMessage.append("\n   Credit:  ");
        terminalMessage.append(c.c_credit);
        terminalMessage.append("\n   %Disc:   ");
        terminalMessage.append(c.c_discount);
        terminalMessage.append("\n   Phone:   ");
        terminalMessage.append(c.c_phone);
        terminalMessage.append("\n\n Amount Paid:      ");
        terminalMessage.append(h_amount);
        terminalMessage.append("\n Credit Limit:     ");
        terminalMessage.append(c.c_credit_lim);
        terminalMessage.append("\n New Cust-Balance: ");
        terminalMessage.append(c.c_balance);
        if (c.c_credit.equals("BC")) {
            if (c_data.length() > 50) {
                terminalMessage.append("\n\n Cust-Data: " + c_data.substring(0, 50));
                int data_chunks = c_data.length() > 200 ? 4 : c_data.length() / 50;
                for (int n = 1; n < data_chunks; n++)
                    terminalMessage.append("\n            " + c_data.substring(n * 50, (n + 1) * 50));
            } else {
                terminalMessage.append("\n\n Cust-Data: " + c_data);
            }
        }
        terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");

        if (LOG.isTraceEnabled())
            LOG.trace(terminalMessage.toString());

    }

    // attention duplicated code across trans... ok for now to maintain separate prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn) throws SQLException {

        payGetCust.setInt(1, c_w_id);
        payGetCust.setInt(2, c_d_id);
        payGetCust.setInt(3, c_id);
        ResultSet rs = payGetCust.executeQuery();
        if (!rs.next()) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        Customer c = TPCCUtil.newCustomerFromResults(rs);
        c.c_id = c_id;
        c.c_last = rs.getString("C_LAST");
        rs.close();
        return c;
    }
    
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, NgCache cafe) throws Exception {
        String queryPayGetCust = String.format(TPCCConfig.QUERY_PAY_GET_CUST, c_w_id, c_d_id, c_id);
        QueryGetCustomerById rs = (QueryGetCustomerById) cafe.readStatement(queryPayGetCust);
        if (rs == null) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }
        Customer c = rs.getCustomer();
        return c;
    }

    // attention this code is repeated in other transacitons... ok for now to allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last) throws SQLException {
        ArrayList<Customer> customers = new ArrayList<Customer>();

        customerByName.setInt(1, c_w_id);
        customerByName.setInt(2, c_d_id);
        customerByName.setString(3, c_last);
        ResultSet rs = customerByName.executeQuery();

        while (rs.next()) {
            Customer c = TPCCUtil.newCustomerFromResults(rs);
            c.c_id = rs.getInt("C_ID");
            c.c_last = c_last;
            customers.add(c);
        }
        rs.close();

        if (customers.size() == 0) {
            throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that
        // counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
    }
    
    public int getCustomerByName(int c_w_id, int c_d_id, String c_last, 
            NgCache cafe) throws Exception {
        String queryPayGetCustByName = String.format(TPCCConfig.QUERY_PAY_GET_CUST_BY_NAME, c_w_id, c_d_id, c_last);
        QueryGetCustomerByNameResult rs = (QueryGetCustomerByNameResult) cafe.readStatement(queryPayGetCustByName);
        if (rs == null) {
            throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }

        List<Integer> customerIds = rs.getCustomerIds();
        if (customerIds.size() == 0) {
            throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that
        // counts starting from 1.
        int index = customerIds.size() / 2;
        if (customerIds.size() % 2 == 0) {
            index -= 1;
        }
        return customerIds.get(index);
    }

    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID, NgCache cafe, Map<String, Object> tres)
                    throws SQLException {
        // initializing all prepared statements
        payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL);
        payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL);
        payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL);
        payGetDist = this.getPreparedStatement(conn, payGetDistSQL);
        payGetCust = this.getPreparedStatement(conn, payGetCustSQL);
        payGetCustCdata = this.getPreparedStatement(conn, payGetCustCdataSQL);
        payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL);
        payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL);
        payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL);
        customerByName = this.getPreparedStatement(conn, customerByNameSQL);

        // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        
        if (TPCCConfig.crossWarehouse) {
            if (x <= 85) {
                customerDistrictID = districtID;
                customerWarehouseID = terminalWarehouseID;
            } else {
                customerDistrictID = TPCCUtil.randomNumber(1, jTPCCConfig.configDistPerWhse, gen);
                do {
                    customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
                } while (customerWarehouseID == terminalWarehouseID && numWarehouses > 1);
            }
        } else {
            customerDistrictID = districtID;
            customerWarehouseID = terminalWarehouseID;
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        boolean customerByName;
        String customerLastName = null;
        customerID = -1;
        if (y <= 60) {
//            // 60% lookups by last name
            customerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);
        
        if (Config.DEBUG) {
            out.println(String.format("Payment w_id=%s, c_w_id=%s, h_amount=%s, d_id=%s, c_d_id=%s, c_id=%s,"
                    + "c_last=%s, c_by_name=%s, cafe=%b", terminalWarehouseID, customerWarehouseID, paymentAmount, 
                    districtID, customerDistrictID, customerID, customerLastName, customerByName, false));
        }
        
//        paymentTransaction(terminalWarehouseID, customerWarehouseID, paymentAmount, 
//                districtID, customerDistrictID, customerID, customerLastName, customerByName, 
//                conn, cafe, tres);
        paymentTransaction2(terminalWarehouseID, customerWarehouseID, paymentAmount, 
                districtID, customerDistrictID, customerID, customerLastName, customerByName, 
                conn, cafe, tres);        

        return null;
    }

    private void paymentTransaction(int w_id, int c_w_id, float h_amount, 
            int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name, 
            Connection conn, NgCache cafe, Map<String, Object> tres) throws SQLException {
        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;
        if (TPCCConstants.DML_Trace) {
            tres.put("count", 0);
        }

        int retry = 0;
        while (true) {
            try {
                cafe.startSession("Payment");
    
                String updateWhse = String.format(TPCCConfig.DML_UPDATE_WHSE, w_id, String.valueOf(h_amount));
                boolean success = cafe.writeStatement(updateWhse);
                if (!success)
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
    
                if (TPCCConstants.DML_Trace) {
                    int count = (Integer) tres.get("count");
                    tres.put("DML" + count, payUpdateWhse.toString());
                    count++;
                    tres.put("count", count);
                }
    
                String queryGetWhse = String.format(TPCCConfig.QUERY_GET_WHSE, w_id);
                QueryGetWhseResult res1 = (QueryGetWhseResult) cafe.readStatement(queryGetWhse);
                if (res1 == null)
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
                w_street_1 = res1.getStreet1();
                w_street_2 = res1.getStreet2();
                w_city = res1.getCity();
                w_state = res1.getState();
                w_zip = res1.getZip();
                w_name = res1.getName();
    
                String updateDist = String.format(TPCCConfig.DML_PAY_UPDATE_DIST, w_id, d_id, h_amount);
                success = cafe.writeStatement(updateDist);
                if (!success)
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
    
                if (TPCCConstants.DML_Trace) {
                    int count = (Integer) tres.get("count");
                    tres.put("DML" + count, payUpdateDist.toString());
                    count++;
                    tres.put("count", count);
                }
    
                String getDist = String.format(TPCCConfig.QUERY_GET_DIST2, w_id, d_id);
                QueryGetDist2Result res2 = (QueryGetDist2Result) cafe.readStatement(getDist);
                if (res2 == null)
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                d_street_1 = res2.getStreet1();
                d_street_2 = res2.getStreet2();
                d_city = res2.getCity();
                d_state = res2.getState();
                d_zip = res2.getZip();
                d_name = res2.getName();
    
                Customer c;
                if (c_by_name) {
                    assert c_id <= 0;
                    c_id = getCustomerByName(c_w_id, c_d_id, c_last, cafe);
                    
                    String queryPayGetCust = String.format(TPCCConfig.QUERY_PAY_GET_CUST, c_w_id, c_d_id, c_id);
                    QueryGetCustomerById rs = (QueryGetCustomerById) cafe.readStatement(queryPayGetCust);
                    c = rs.getCustomer();
                } else {
                    assert c_last == null;
                    c = getCustomerById(c_w_id, c_d_id, c_id, cafe);
                }
    
                String readBalance = TPCCConstants.DECIMAL_FORMAT.format(c.c_balance);
                float readYTD_payment = c.c_ytd_payment;
                int readPayment_cnt = c.c_payment_cnt;
                c.c_balance = c.c_balance - h_amount;
                c.c_ytd_payment += h_amount;
                c.c_payment_cnt += 1;
                String c_data = null;
                String c_balance = TPCCConstants.DECIMAL_FORMAT.format(c.c_balance);
                if (c.c_credit == null) {
                    System.out.println("Here");
                }
                if (c.c_credit.equals("BC")) { // bad credit
                    String queryGetCustCdata = String.format(TPCCConfig.QUERY_GET_CUST_C_DATA, w_id, d_id, c.c_id);
                    QueryGetCustCDataResult res3 = (QueryGetCustCDataResult)cafe.readStatement(queryGetCustCdata);
                    if (res3 == null)
                        throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");
                    c_data = res3.getCData();
    
                    c_data = c.c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id + " " + h_amount + " | " + c_data;
                    if (c_data.length() > 500)
                        c_data = c_data.substring(0, 500);
    
                    String updateCustBalCdata = String.format(TPCCConfig.DML_UPDATE_CUST_BAL_C_DATA, 
                            c_w_id, c_d_id, c.c_id, c_balance, c.c_ytd_payment, c.c_payment_cnt, c_data);
                    success = cafe.writeStatement(updateCustBalCdata);
                    if (!success)
                        throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id);
    
                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, payUpdateCustBalCdata.toString());
                        count++;
                        tres.put("count", count);
                    }
    
                } else { // GoodCredit
                    String updateCustBal = String.format(TPCCConfig.DML_UPDATE_CUST_BAL, 
                            c_w_id, c_d_id, c.c_id, c_balance, c.c_ytd_payment, c.c_payment_cnt);
                    success = cafe.writeStatement(updateCustBal);
                    if (!success)
                        throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");
    
                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, payUpdateCustBal.toString());
                        count++;
                        tres.put("count", count);
                    }
                }
    
                if (w_name.length() > 10)
                    w_name = w_name.substring(0, 10);
                if (d_name.length() > 10)
                    d_name = d_name.substring(0, 10);
                String h_data = w_name + "    " + d_name;
    
                String insertHist = String.format(TPCCConfig.DML_INSERT_HISTORY, 
                        c_w_id, c_d_id, c.c_id, d_id, w_id, System.currentTimeMillis(), h_amount, h_data);
                success = cafe.writeStatement(insertHist);
    
                if (TPCCConstants.DML_Trace) {
                    int count = (Integer) tres.get("count");
                    tres.put("DML" + count, payInsertHist.toString());
                    count++;
                    tres.put("count", count);
                }
    
                conn.commit();
                cafe.commitSession();
                
                if (Config.ENABLE_LOGGING) {
                    if (c.c_credit.equals("BC")) {
                        tres.put("c_data", c_data);
    
                    }
                    tres.put("w_id", c_w_id);
                    tres.put("d_id", c_d_id);
                    tres.put("c_id", c.c_id);
                    tres.put("c_balance", c_balance);
                    tres.put("c_ytd_payment", c.c_ytd_payment);
                    tres.put("c_payment_cnt", c.c_payment_cnt);
    
                    tres.put("readBalance", readBalance);
                    tres.put("readYTD_payment", readYTD_payment);
                    tres.put("readPayment_cnt", readPayment_cnt);
                }           
                
                if (Config.DEBUG) {
                    System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
                }
                break;
            } catch (Exception e) {
//                e.printStackTrace(System.out);
                
                if (e instanceof COException) {
                    cafe.getStats().incr(((COException) e).getKey());
                }
                
                try { 
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                //throw new UserAbortException("Some error happens. "+ e.getMessage());
                
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        
            sleepRetry();
            retry++;
        }
        
        cafe.getStats().incr("retry"+retry);
    }
    
    private void paymentTransaction2(int w_id, int c_w_id, float h_amount, 
            int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name, 
            Connection conn, NgCache cafe, Map<String, Object> tres) throws SQLException {
        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;
        if (TPCCConstants.DML_Trace) {
            tres.put("count", 0);
        }

        int retry = 0;
        while (true) {
            try {
                cafe.startSession("Payment", String.valueOf(w_id));
                
                String updateWhse = String.format(TPCCConfig.DML_UPDATE_WHSE, w_id, String.valueOf(h_amount));
                boolean success = cafe.writeStatement(updateWhse);
                if (!success)
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
    
                if (TPCCConstants.DML_Trace) {
                    int count = (Integer) tres.get("count");
                    tres.put("DML" + count, payUpdateWhse.toString());
                    count++;
                    tres.put("count", count);
                }
    
                String[] queries = new String[3];
                String queryGetWhse = String.format(TPCCConfig.QUERY_GET_WHSE, w_id);
                queries[0] = queryGetWhse;
                String getDist = String.format(TPCCConfig.QUERY_GET_DIST2, w_id, d_id);
                queries[1] = getDist;
                if (c_by_name) {
                    assert c_id <= 0;
                    String queryPayGetCustByName = String.format(TPCCConfig.QUERY_PAY_GET_CUST_BY_NAME, c_w_id, c_d_id, c_last);
                    queries[2] = queryPayGetCustByName;
                } else {
                    assert c_last == null;
                    String queryPayGetCust = String.format(TPCCConfig.QUERY_PAY_GET_CUST, c_w_id, c_d_id, c_id);
                    queries[2] = queryPayGetCust;
                }
                QueryResult[] results = cafe.readStatements(queries);
                
                QueryGetWhseResult res1 = (QueryGetWhseResult) results[0];
                if (res1 == null)
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
                w_street_1 = res1.getStreet1();
                w_street_2 = res1.getStreet2();
                w_city = res1.getCity();
                w_state = res1.getState();
                w_zip = res1.getZip();
                w_name = res1.getName();
    
                String updateDist = String.format(TPCCConfig.DML_PAY_UPDATE_DIST, w_id, d_id, h_amount);
                success = cafe.writeStatement(updateDist);
                if (!success)
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
    
                if (TPCCConstants.DML_Trace) {
                    int count = (Integer) tres.get("count");
                    tres.put("DML" + count, payUpdateDist.toString());
                    count++;
                    tres.put("count", count);
                }
    
                QueryGetDist2Result res2 = (QueryGetDist2Result) results[1];
                if (res2 == null)
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                d_street_1 = res2.getStreet1();
                d_street_2 = res2.getStreet2();
                d_city = res2.getCity();
                d_state = res2.getState();
                d_zip = res2.getZip();
                d_name = res2.getName();
    
                Customer c;
                if (c_by_name) {
                    assert c_id <= 0;
                    List<Integer> custIds = ((QueryGetCustomerByNameResult) results[2]).getCustomerIds();
                    int index = custIds.size() / 2;
                    if (custIds.size() % 2 == 0) {
                        index -= 1;
                    }
                    c_id = custIds.get(index);
                    
                    String queryPayGetCust = String.format(TPCCConfig.QUERY_PAY_GET_CUST, c_w_id, c_d_id, c_id);
                    QueryGetCustomerById rs = (QueryGetCustomerById) cafe.readStatement(queryPayGetCust);
                    c = rs.getCustomer();
                } else {
                    assert c_last == null;
                    QueryGetCustomerById rs = (QueryGetCustomerById) results[2];
                    if (rs == null) {
                        throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
                                + " C_W_ID=" + c_w_id + " not found!");
                    }
                    c = rs.getCustomer();
                }
    
                String readBalance = TPCCConstants.DECIMAL_FORMAT.format(c.c_balance);
                float readYTD_payment = c.c_ytd_payment;
                int readPayment_cnt = c.c_payment_cnt;
                c.c_balance = c.c_balance - h_amount;
                c.c_ytd_payment += h_amount;
                c.c_payment_cnt += 1;
                String c_data = null;
                String c_balance = TPCCConstants.DECIMAL_FORMAT.format(c.c_balance);
                if (c.c_credit == null) {
                    System.out.println("Here");
                }
                if (c.c_credit.equals("BC")) { // bad credit
                    String queryGetCustCdata = String.format(TPCCConfig.QUERY_GET_CUST_C_DATA, w_id, d_id, c_id);
                    QueryGetCustCDataResult res3 = (QueryGetCustCDataResult) cafe.readStatement(queryGetCustCdata);
                    if (res3 == null)
                        throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");
                    c_data = res3.getCData();
    
                    c_data = c.c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id + " " + h_amount + " | " + c_data;
                    if (c_data.length() > 500)
                        c_data = c_data.substring(0, 500);
    
                    String updateCustBalCdata = String.format(TPCCConfig.DML_UPDATE_CUST_BAL_C_DATA, 
                            c_w_id, c_d_id, c.c_id, c_balance, c.c_ytd_payment, c.c_payment_cnt, c_data);
                    success = cafe.writeStatement(updateCustBalCdata);
                    if (!success)
                        throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id);
    
                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, payUpdateCustBalCdata.toString());
                        count++;
                        tres.put("count", count);
                    }
    
                } else { // GoodCredit
                    String updateCustBal = String.format(TPCCConfig.DML_UPDATE_CUST_BAL, 
                            c_w_id, c_d_id, c.c_id, c_balance, c.c_ytd_payment, c.c_payment_cnt);
                    success = cafe.writeStatement(updateCustBal);
                    if (!success)
                        throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");
    
                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, payUpdateCustBal.toString());
                        count++;
                        tres.put("count", count);
                    }
                }
    
                if (w_name.length() > 10)
                    w_name = w_name.substring(0, 10);
                if (d_name.length() > 10)
                    d_name = d_name.substring(0, 10);
                String h_data = w_name + "    " + d_name;
    
                String insertHist = String.format(TPCCConfig.DML_INSERT_HISTORY, 
                        c_w_id, c_d_id, c.c_id, d_id, w_id, System.currentTimeMillis(), h_amount, h_data);
                success = cafe.writeStatement(insertHist);
    
                if (TPCCConstants.DML_Trace) {
                    int count = (Integer) tres.get("count");
                    tres.put("DML" + count, payInsertHist.toString());
                    count++;
                    tres.put("count", count);
                }
    
                if (cafe.validateSession()) {
                    conn.commit();
                    cafe.commitSession();
                } else {
                    conn.rollback();
                    cafe.abortSession();
                }
                
                if (Config.ENABLE_LOGGING) {
                    if (c.c_credit.equals("BC")) {
                        tres.put("c_data", c_data);
    
                    }
                    tres.put("w_id", c_w_id);
                    tres.put("d_id", c_d_id);
                    tres.put("c_id", c.c_id);
                    tres.put("c_balance", c_balance);
                    tres.put("c_ytd_payment", c.c_ytd_payment);
                    tres.put("c_payment_cnt", c.c_payment_cnt);
    
                    tres.put("readBalance", readBalance);
                    tres.put("readYTD_payment", readYTD_payment);
                    tres.put("readPayment_cnt", readPayment_cnt);
                }           
                
                if (Config.DEBUG) {
                    System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
                }
                break;
            } catch (Exception e) {
//                e.printStackTrace(System.out);
                
                if (e instanceof COException) {
//                    cafe.getStats().incr(((COException) e).getKey());
//                    System.out.println(((COException) e).getKey());
                }
                
                try { 
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                //throw new UserAbortException("Some error happens. "+ e.getMessage());
                
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        
            sleepRetry();
            retry++;
        }
        
        cafe.getStats().incr("retry"+retry);
    }    
}
