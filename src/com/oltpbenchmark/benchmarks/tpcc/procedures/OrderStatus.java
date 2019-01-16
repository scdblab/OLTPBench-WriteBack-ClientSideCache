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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustomerById;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustomerByNameResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryOrdStatGetNewestOrdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryOrderStatGetOrderLinesResult;
import com.usc.dblab.cafe.NgCache;

public class OrderStatus extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(OrderStatus.class);

    public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + TPCCConstants.TABLENAME_OPENORDER
            + " WHERE O_W_ID = ?"
            + " AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1");

    public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt("SELECT OL_NUMBER , OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY,"
            + " OL_AMOUNT, OL_DELIVERY_D"
            + " FROM " + TPCCConstants.TABLENAME_ORDERLINE
            + " WHERE OL_O_ID = ?"
            + " AND OL_D_ID =?"
            + " AND OL_W_ID = ?");

    public SQLStmt payGetCustSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
            + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
            + "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");

    public SQLStmt customerByNameSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
            + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER
            + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST");

    public PreparedStatement ordStatGetNewestOrd = null;
    public PreparedStatement ordStatGetOrderLines = null;
    public PreparedStatement payGetCust = null;
    public PreparedStatement customerByName = null;


    public ResultSet run(Connection conn, Random gen,
            int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            Map<String, Object> tres) throws SQLException{


        //initializing all prepared statements
        payGetCust =this.getPreparedStatement(conn, payGetCustSQL);
        customerByName=this.getPreparedStatement(conn, customerByNameSQL);
        ordStatGetNewestOrd =this.getPreparedStatement(conn, ordStatGetNewestOrdSQL);
        ordStatGetOrderLines=this.getPreparedStatement(conn, ordStatGetOrderLinesSQL);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        boolean isCustomerByName=false;
        int y = TPCCUtil.randomNumber(1, 100, gen);
        String customerLastName = null;
        int customerID = -1;
        if (y <= 60) {
            isCustomerByName = true;
            customerLastName = TPCCUtil
                    .getNonUniformRandomLastNameForRun(gen);
        } else {
            isCustomerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        if (Config.DEBUG) {
            out.println(String.format("Order Status d_id=%d, y=%d, c_lastname=%s, c_id=%d", districtID, y, customerLastName, customerID));
        }

        orderStatusTransaction(terminalWarehouseID, districtID,
                customerID, customerLastName, isCustomerByName, conn, tres);
        return null;
    }

    // attention duplicated code across trans... ok for now to maintain separate prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, 
            Connection conn)
                    throws SQLException {

        payGetCust.setInt(1, c_w_id);
        payGetCust.setInt(2, c_d_id);
        payGetCust.setInt(3, c_id);
        ResultSet rs = payGetCust.executeQuery();
        if (!rs.next()) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }

        Customer c = TPCCUtil.newCustomerFromResults(rs);
        c.c_id = c_id;
        c.c_last = rs.getString("C_LAST");
        rs.close();
        return c;
    }

    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, 
            NgCache cafe)
                    throws Exception {
        String queryPayGetCust = String.format(TPCCConfig.QUERY_PAY_GET_CUST, c_w_id, c_d_id, c_id);
        QueryGetCustomerById rs = (QueryGetCustomerById) cafe.readStatement(queryPayGetCust);
        if (rs == null) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }
        Customer c = rs.getCustomer();
        c.c_id = c_id;
        c.c_last = c.c_last;
        return c;
    }

    private void orderStatusTransaction(int w_id, int d_id, int c_id,
            String c_last, boolean c_by_name, Connection conn, Map<String, Object> tres) throws SQLException {
        int o_id = -1, o_carrier_id = -1;
        Timestamp entdate;
        ArrayList<String> orderLines = new ArrayList<String>();

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            // TODO: This only needs c_balance, c_first, c_middle, c_id
            // only fetch those columns?
            c = getCustomerByName(w_id, d_id, c_last);
        } else {
            assert c_last == null;
            c = getCustomerById(w_id, d_id, c_id,conn);
        }

        // find the newest order for the customer
        // retrieve the carrier & order date for the most recent order.
        ordStatGetNewestOrd.setInt(1, w_id);
        ordStatGetNewestOrd.setInt(2, d_id);
        ordStatGetNewestOrd.setInt(3, c.c_id);
        ResultSet rs = ordStatGetNewestOrd.executeQuery();

        if (!rs.next()) {
            throw new RuntimeException("No orders for O_W_ID=" + w_id
                    + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
        }

        o_id = rs.getInt("O_ID");
        o_carrier_id = rs.getInt("O_CARRIER_ID");
        entdate = rs.getTimestamp("O_ENTRY_D");
        rs.close();
        rs = null;


        // retrieve the order lines for the most recent order


        ordStatGetOrderLines.setInt(1, o_id);
        ordStatGetOrderLines.setInt(2, d_id);
        ordStatGetOrderLines.setInt(3, w_id);
        rs = ordStatGetOrderLines.executeQuery();
        int orderlinesCount=0;
        long deliveryDate=-1;

        while (rs.next()) {
            orderlinesCount++;
            StringBuilder orderLine = new StringBuilder();
            orderLine.append("[");
            int ol_number=rs.getInt("OL_NUMBER");

            orderLine.append(rs.getLong("OL_SUPPLY_W_ID"));
            orderLine.append(" - ");
            orderLine.append(rs.getLong("OL_I_ID"));
            orderLine.append(" - ");
            orderLine.append(rs.getLong("OL_QUANTITY"));
            orderLine.append(" - ");
            orderLine.append(TPCCUtil.formattedDouble(rs
                    .getDouble("OL_AMOUNT")));
            orderLine.append(" - ");
            if (rs.getTimestamp("OL_DELIVERY_D") != null){
                orderLine.append(rs.getTimestamp("ol_delivery_d"));
                deliveryDate=rs.getTimestamp("ol_delivery_d").getTime();


            }
            else{
                orderLine.append("99-99-9999");
                deliveryDate=-999;
            }

            if (Config.ENABLE_LOGGING){
                tres.put(Integer.toString(ol_number-1),deliveryDate);
            }
            orderLine.append("]");
            orderLines.add(orderLine.toString());
        }
        rs.close();
        rs = null;
        // populate log info
        if (Config.ENABLE_LOGGING){
            //   tres.put("ol_delivery_d",deliveryDate);
            tres.put("c_id",c.c_id);
            tres.put("c_balance",c.c_balance);
            tres.put("c_payment_cnt", c.c_payment_cnt);
            tres.put("c_ytd_payment",c.c_ytd_payment);
            tres.put("w_id",w_id);
            tres.put("d_id", d_id);
            tres.put("ol_count", orderlinesCount);
            tres.put("o_id",o_id);
            tres.put("o_carrier_id",o_carrier_id);
            tres.put("ol_delivery_d",deliveryDate);

        }

        if (Config.DEBUG) {
            System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
        }

        // commit the transaction
        conn.commit();

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n");
        terminalMessage
        .append("+-------------------------- ORDER-STATUS -------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n\n Customer:  ");
        terminalMessage.append(c.c_id);
        terminalMessage.append("\n   Name:    ");
        terminalMessage.append(c.c_first);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_middle);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_last);
        terminalMessage.append("\n   Balance: ");
        terminalMessage.append(c.c_balance);
        terminalMessage.append("\n\n");
        if (o_id == -1) {
            terminalMessage.append(" Customer has no orders placed.\n");
        } else {
            terminalMessage.append(" Order-Number: ");
            terminalMessage.append(o_id);
            terminalMessage.append("\n    Entry-Date: ");
            terminalMessage.append(entdate);
            terminalMessage.append("\n    Carrier-Number: ");
            terminalMessage.append(o_carrier_id);
            terminalMessage.append("\n\n");
            if (orderLines.size() != 0) {
                terminalMessage
                .append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
                for (String orderLine : orderLines) {
                    terminalMessage.append(" ");
                    terminalMessage.append(orderLine);
                    terminalMessage.append("\n");
                }
            } else {
                if(LOG.isTraceEnabled()) LOG.trace(" This Order has no Order-Lines.\n");
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");
        if(LOG.isTraceEnabled()) LOG.trace(terminalMessage.toString());
    }

    //attention this code is repeated in other transacitons... ok for now to allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last)
            throws SQLException {
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
            throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
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
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        boolean isCustomerByName=false;
        int y = TPCCUtil.randomNumber(1, 100, gen);
        String customerLastName = null;
        int customerID = -1;
        if (y <= 60) {
            isCustomerByName = true;
            customerLastName = TPCCUtil
                    .getNonUniformRandomLastNameForRun(gen);
        } else {
            isCustomerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }
        
        if (Config.DEBUG) {
            out.println(String.format("Order Status d_id=%d, y=%d, c_lastname=%s, c_id=%d", districtID, y, customerLastName, customerID));
        }

        orderStatusTransaction(terminalWarehouseID, districtID,
                customerID, customerLastName, isCustomerByName, conn, cafe, tres);
        return null;
    }

    public void orderStatusTransaction(int w_id, int d_id, int c_id,
            String c_last, boolean c_by_name, Connection conn, 
            NgCache cafe, Map<String, Object> tres) throws SQLException {
        int o_id = -1, o_carrier_id = -1;
        Timestamp entdate;
        ArrayList<String> orderLines = new ArrayList<String>();

        int retry = 0;
        while (true) {
            try {
                cafe.startSession("OrderStatus", String.valueOf(w_id));
                
                Customer c;
                if (c_by_name) {
                    assert c_id <= 0;
                    // TODO: This only needs c_balance, c_first, c_middle, c_id
                    // only fetch those columns?
                    c_id = getCustomerByName(w_id, d_id, c_last, cafe);
                    
                    String queryPayGetCust = String.format(TPCCConfig.QUERY_PAY_GET_CUST, w_id, d_id, c_id);
                    QueryGetCustomerById rs = (QueryGetCustomerById) cafe.readStatement(queryPayGetCust);
                    c = rs.getCustomer();
                } else {
                    assert c_last == null;
                    c = getCustomerById(w_id, d_id, c_id, cafe);
                }

                // find the newest order for the customer
                // retrieve the carrier & order date for the most recent order.
                String queryOrdStatGetNewestOrd = String.format(TPCCConfig.QUERY_ORDER_STAT_GET_NEWEST_ORDER, w_id, d_id, c.c_id);
                QueryOrdStatGetNewestOrdResult rs = (QueryOrdStatGetNewestOrdResult) cafe.readStatement(queryOrdStatGetNewestOrd);
                if (rs == null) {
                    throw new RuntimeException("No orders for O_W_ID=" + w_id
                            + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
                }

                o_id = rs.getOrderId();
                o_carrier_id = rs.getCarrierId();
                entdate = rs.getTimestamp();

                // retrieve the order lines for the most recent order
                String queryOrdStatGetOrderLines = String.format(TPCCConfig.QUERY_ORDER_STAT_GET_ORDER_LINES, w_id, d_id, o_id);
                QueryOrderStatGetOrderLinesResult res = (QueryOrderStatGetOrderLinesResult) cafe.readStatement(queryOrdStatGetOrderLines);
                int orderlinesCount=0;
                long deliveryDate=-1;

                orderlinesCount = res.getOrderLinesCount();
                deliveryDate = res.getDeliveryDate();

                //			while (rs.next()) {
                //				orderlinesCount++;
                //				StringBuilder orderLine = new StringBuilder();
                //				orderLine.append("[");
                //				int ol_number=rs.getInt("OL_NUMBER");
                //
                //				orderLine.append(rs.getLong("OL_SUPPLY_W_ID"));
                //				orderLine.append(" - ");
                //				orderLine.append(rs.getLong("OL_I_ID"));
                //				orderLine.append(" - ");
                //				orderLine.append(rs.getLong("OL_QUANTITY"));
                //				orderLine.append(" - ");
                //				orderLine.append(TPCCUtil.formattedDouble(rs
                //						.getDouble("OL_AMOUNT")));
                //				orderLine.append(" - ");
                //				if (rs.getTimestamp("OL_DELIVERY_D") != null){
                //					orderLine.append(rs.getTimestamp("ol_delivery_d"));
                //					deliveryDate=rs.getTimestamp("ol_delivery_d").getTime();
                //				} else{
                //					orderLine.append("99-99-9999");
                //					deliveryDate=-999;
                //				}
                //
                //				if (TPCCConstants.ENABLE_LOGGING){
                //					tres.put(Integer.toString(ol_number-1),deliveryDate);
                //				}
                //				orderLine.append("]");
                //				orderLines.add(orderLine.toString());
                //			}


                // populate log info
                if (Config.ENABLE_LOGGING){
                    //   tres.put("ol_delivery_d",deliveryDate);
                    tres.put("c_id",c.c_id);
                    tres.put("c_balance",c.c_balance);
                    tres.put("c_payment_cnt", c.c_payment_cnt);
                    tres.put("c_ytd_payment",c.c_ytd_payment);
                    tres.put("w_id",w_id);
                    tres.put("d_id", d_id);
                    tres.put("ol_count", orderlinesCount);
                    tres.put("o_id",o_id);
                    tres.put("o_carrier_id",o_carrier_id);
                    tres.put("ol_delivery_d",deliveryDate);
                }

                if (Config.DEBUG) {
                    System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
                }

                // commit the transaction
                if (cafe.validateSession()) {
                    conn.commit();
                    cafe.commitSession();
                } else {
                    conn.rollback();
                    cafe.abortSession();
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



