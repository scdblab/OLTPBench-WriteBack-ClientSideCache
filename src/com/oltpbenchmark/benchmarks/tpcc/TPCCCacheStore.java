package com.oltpbenchmark.benchmarks.tpcc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.procedures.QueryResultRangeGetItems;
import com.oltpbenchmark.benchmarks.tpcc.procedures.QueryStockGetCountItems;
import com.oltpbenchmark.benchmarks.tpcc.procedures.QueryStockGetItemIDs;
import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustCDataResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustIdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustWhseResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustomerById;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustomerByNameResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetDist2Result;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetDistResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetItemResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetOrderIdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetStockCountResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetStockResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetSumOrderAmountResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetWhseResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryOrdStatGetNewestOrdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryOrderStatGetOrderLinesResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryStockItemsEqualThreshold;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.CacheEntry;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.QueryResult;

import edu.usc.dblab.intervaltree.Interval1D;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConfig.*;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConstants.*;

public class TPCCCacheStore extends CacheStore {
    private static final String SET = "S";
    private static final String INCR = "P";
    private static final String INCR_OR_SET = "O";
    private static final String ADD = "A";
    private static final String REMOVE_FIRST = "R";
    private static final String RMV = "D";
    private static final String CHECK = "C";
    
    private static final Logger logger = Logger.getLogger(TPCCCacheStore.class);
    
    private static void printMap(Map<String, Double> resultMap) {
        double total = 0;
        int cnt = 0;
        for (String key: resultMap.keySet()) {
            System.out.print(key+": "+resultMap.get(key)+";");
            cnt++;
            total += resultMap.get(key);
        }
        System.out.println("\nAverage: "+ total / (double)cnt);
    }

    private Connection conn;

    private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();

    // stock level
    public SQLStmt stockGetDistOrderIdSQL = new SQLStmt("SELECT D_NEXT_O_ID, D_TAX FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?");
    public SQLStmt stockGetCountStockSQL = new SQLStmt("SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT"
            + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK
            + " WHERE OL_W_ID = ?"
            + " AND OL_D_ID = ?"
            + " AND OL_O_ID < ?"
            + " AND OL_O_ID >= ? - 20"
            + " AND S_W_ID = ?"
            + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?");

    // status order
    public SQLStmt customerByNameSQL = new SQLStmt("SELECT C_ID FROM " + TPCCConstants.TABLENAME_CUSTOMER
            + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST");
    public SQLStmt payGetCustSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
            + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
            + "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + TPCCConstants.TABLENAME_OPENORDER
            + " WHERE O_W_ID = ?"
            + " AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1");
    public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt("SELECT OL_NUMBER , OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY,"
            + " OL_AMOUNT, OL_DELIVERY_D"
            + " FROM " + TPCCConstants.TABLENAME_ORDERLINE
            + " WHERE OL_O_ID = ?"
            + " AND OL_D_ID =?"
            + " AND OL_W_ID = ?");

    // new order
    public final SQLStmt stmtGetCustWhseSQL = new SQLStmt("SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX" + "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + ", " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ? AND C_W_ID = ?" + " AND C_D_ID = ? AND C_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt("SELECT D_NEXT_O_ID, D_TAX FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

    public final SQLStmt stmtInsertNewOrderSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER + " (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ( ?, ?, ?)");

    public final SQLStmt stmtUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = ? AND D_ID = ?");

    public final SQLStmt stmtInsertOOrderSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER + " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" + " VALUES (?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt stmtGetItemSQL = new SQLStmt("SELECT I_PRICE, I_NAME , I_DATA FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE I_ID = ?");

    public final SQLStmt stmtGetStockSQL = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " + "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" + " FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_I_ID = ? AND S_W_ID = ? FOR UPDATE");

    public final SQLStmt stmtUpdateStockSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_STOCK + " SET S_QUANTITY = CASE WHEN S_QUANTITY-? >= 10 THEN S_QUANTITY-? ELSE S_QUANTITY+91-? END, "
            + "S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? " + " WHERE S_I_ID = ? AND S_W_ID = ?");

    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID," + "  OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?,?,?,?,?,?,?,?,?)");

    //payment
    public SQLStmt payUpdateWhseSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET W_YTD = W_YTD + ?  WHERE W_ID = ? ");
    public SQLStmt payGetWhseSQL = new SQLStmt("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" + " FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ?");
    public SQLStmt payUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?");
    public SQLStmt payGetDistSQL = new SQLStmt("SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" + " FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?");
    public SQLStmt payGetCustCdataSQL = new SQLStmt("SELECT C_DATA FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ?, C_DATA = ? " + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payUpdateCustBalSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payInsertHistSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " + " VALUES (?,?,?,?,?,?,?,?)");    
    public SQLStmt delivGetOrderIdSQL = new SQLStmt("SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = ?" + " AND NO_W_ID = ? ORDER BY NO_O_ID");
    public SQLStmt delivDeleteNewOrderSQL = new SQLStmt("DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER + "" + " WHERE NO_O_ID = ? AND NO_D_ID = ?" + " AND NO_W_ID = ?");
    public SQLStmt delivGetCustIdSQL = new SQLStmt("SELECT O_C_ID" + " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?");
    public SQLStmt delivUpdateCarrierIdSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = ?" + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?");
    public SQLStmt delivUpdateDeliveryDateSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = ?" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?");
    public SQLStmt delivSumOrderAmountSQL = new SQLStmt("SELECT SUM(OL_AMOUNT) AS OL_TOTAL" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?");
    public SQLStmt delivUpdateCustBalDelivCntSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = C_BALANCE + ?" + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1" + " WHERE C_W_ID = ?" + " AND C_D_ID = ?" + " AND C_ID = ?");
    
    public static final int SQL_INGROUP=11;
    public SQLStmt stockGetItemIdsSQL = new SQLStmt("SELECT OL_O_ID, OL_I_ID FROM " + TPCCConstants.TABLENAME_ORDERLINE + " WHERE OL_O_ID < ? AND OL_O_ID >= ? - 20" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?");
    public SQLStmt stockCountItemsIdsSQL = new SQLStmt("SELECT COUNT(*) FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_W_ID = ?" + " AND S_I_ID IN (?,?,?,?,?,?,?,?,?,?)" + " AND S_QUANTITY < ?");
    public SQLStmt stockItemsIdsEqualThresholdSQL = 
            new SQLStmt("SELECT S_I_ID FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_W_ID = ?" + " AND S_QUANTITY = ?");
    
    public TPCCCacheStore(Connection conn) {
        this.conn = conn;
    }

    @Override
    public LinkedHashMap<String, Delta> updateCacheEntries(String dml, Set<String> keys) {
        LinkedHashMap<String, Delta> map = new LinkedHashMap<>();
        if (keys.size() == 0) return map;

        String[] tokens = dml.split(",");
        Delta d = null;
        String s;
        switch (tokens[0]) {
        case DML_UPDATE_DIST_PRE:
            d = new Delta(Change.TYPE_RMW, String.format("%s,next_o_id,1", INCR));
            map.put(String.format(KEY_DIST, tokens[1], tokens[2]), d);
            d = new Delta(Change.TYPE_APPEND, Integer.parseInt(tokens[3]));
            map.put(String.format(KEY_NEW_ORDER_IDS, tokens[1], tokens[2]), d);
            break;
        case DML_INSERT_OORDER_PRE:
            s = String.format("%s,o_id,%s;%s,o_carrier_id,%s;%s,o_delivery_date,%s", 
                    SET, tokens[4], SET, 0, SET, tokens[5]);
            d = new Delta(Change.TYPE_SET, s);
            map.put(String.format(KEY_LAST_ORDER, tokens[1], tokens[2], tokens[3]), d);
            
            s = String.format("%s,c_id,%s", SET, tokens[3]);
            d = new Delta(Change.TYPE_SET, s);
            map.put(String.format(KEY_CUSTOMERID_ORDER, tokens[1], tokens[2], tokens[4]), d);
            
            s = String.format("%s,dev_date,%s;%s,ol_cnt,%s", SET, 0, SET, tokens[6]);
            d = new Delta(Change.TYPE_SET, s);
            map.put(String.format(KEY_ORDER_LINES, tokens[1], tokens[2], tokens[4]), d);
            break;
        case DML_INSERT_ORDER_LINE_PRE:
            s = String.format("%s,ol_amount,%s", INCR_OR_SET, tokens[8]);
            d = new Delta(Change.TYPE_SET, s);
            map.put(String.format(KEY_OL_AMOUNT, tokens[1], tokens[2], tokens[3]), d);
            
            s = String.format("%s,o_id,%s,i_id,%s;%s", ADD, tokens[3], tokens[5], REMOVE_FIRST);
            d = new Delta(Change.TYPE_RMW, s);
            map.put(String.format(KEY_STOCK_LAST20ORDERS_ITEM_IDS, tokens[1], tokens[2]), d);
            break;
        case DML_DELETE_NEW_ORDER_PRE:
            d = new Delta(Change.TYPE_RMW, Integer.parseInt(tokens[3]));
            map.put(String.format(KEY_NEW_ORDER_IDS, tokens[1], tokens[2], tokens[3]), d);
            break;
        case DML_UPDATE_STOCK_PRE:
            logger.debug(String.format("Update s_quantity of item id %s from %s to %s", tokens[2], tokens[3], tokens[7]));
            s = String.format("%s,s_quantity,%s;%s,s_ytd,%s;%s,s_order_cnt,%s;%s,s_remote_cnt,%s", 
                    SET, tokens[3], INCR, tokens[4], INCR, tokens[5], INCR, tokens[6]);
            d = new Delta(Delta.TYPE_RMW, s);
            map.put(String.format(KEY_STOCK, tokens[1], tokens[2]), d);
            
            if (!TPCCConfig.useRangeQC) {
                int quantity = Integer.parseInt(tokens[3]);
                if (10 <= quantity && quantity <= 20) {
                    d = new Delta(Delta.TYPE_APPEND, Integer.parseInt(tokens[2]));
                    map.put(String.format(KEY_STOCK_ITEMS_EQUAL_THRESHOLD, tokens[1], tokens[3]), d);
                }
                
                quantity = Integer.parseInt(tokens[7]);
                if (10 <= quantity && quantity <= 20) {
                    d = new Delta(Delta.TYPE_RMW, Integer.parseInt(tokens[2]));
                    map.put(String.format(KEY_STOCK_ITEMS_EQUAL_THRESHOLD, tokens[1], tokens[7]), d);
                }
            }
            break;
        case DML_UPDATE_CUST_BAL_C_DATA_PRE:
            s = String.format("%s,c_balance,%s;%s,c_ytd_payment,%s;%s,c_payment_cnt,%s",
                    SET, tokens[4], SET, tokens[5], SET, tokens[6]);
            d = new Delta(Delta.TYPE_RMW, s);
            map.put(String.format(KEY_CUSTOMERID, tokens[1], tokens[2], tokens[3]), d);
            
            d = new Delta(Delta.TYPE_RMW, String.format("%s,c_data,%s", SET, tokens[7]));
            map.put(String.format(KEY_CUSTOMER_DATA, tokens[1], tokens[2], tokens[3]), d);
            break;
        case DML_UPDATE_CUST_BAL_PRE:
            s = String.format("%s,c_balance,%s;%s,c_ytd_payment,%s;%s,c_payment_cnt,%s",
                    SET, tokens[4], SET, tokens[5], SET, tokens[6]);
            d = new Delta(Delta.TYPE_RMW, s);
            map.put(String.format(KEY_CUSTOMERID, tokens[1], tokens[2], tokens[3]), d);
            break;
        // Delivery
        case DML_UPDATE_CUST_BAL_DELIVERY_CNT_PRE:
            s = String.format("%s,c_balance,%s;%s,c_delivery_cnt,%s",
                    INCR, tokens[4], INCR, 1);
            d = new Delta(Delta.TYPE_RMW, s);
            map.put(String.format(KEY_CUSTOMERID, tokens[1], tokens[2], tokens[3]), d);
            break;
        case DML_UPDATE_DELIVERY_DATE_PRE:
            s = String.format("%s,dev_date,%s", SET, tokens[4]);
            d = new Delta(Delta.TYPE_RMW, s);
            map.put(String.format(KEY_ORDER_LINES, tokens[1], tokens[2], tokens[3]), d);
            break;
        case DML_UPDATE_CARRIER_ID_PRE:
            s = String.format("%s,o_carrier_id,%s;%s,o_id,%s", SET, tokens[4], CHECK, tokens[3]);
            d = new Delta(Change.TYPE_RMW, s);
            map.put(String.format(KEY_LAST_ORDER, tokens[1], tokens[2], tokens[5]), d);
            break;
        }

        return map;
    }

    @Override
    public Set<String> getReferencedKeysFromQuery(String query) {
        String[] tokens = query.split(",");
        Set<String> keys = new HashSet<>();

        switch (tokens[0]) {
        // NewOrder
        case QUERY_GET_CUST_WHSE_PRE:
            keys.add(String.format(KEY_CUSTWHSE, tokens[1], tokens[2], tokens[3]));
            break;
        case QUERY_DISTRICT_NEXT_ORDER_PRE:
            keys.add(String.format(KEY_DIST, tokens[1], tokens[2]));
            break;
        case QUERY_GET_ITEM_PRE:
            keys.add(String.format(KEY_ITEM, tokens[1]));
            break;
        case QUERY_GET_STOCK_PRE:
            keys.add(String.format(KEY_STOCK, tokens[1], tokens[2]));
            break;
            
        // Payment
        case QUERY_GET_WHSE_PRE:
            keys.add(String.format(KEY_WAREHOUSE, tokens[1]));
            break;
        case QUERY_GET_DIST2_PRE:
            keys.add(String.format(KEY_DIST2, tokens[1], tokens[2]));
            break;
        case QUERY_PAY_GET_CUST_BY_NAME_PRE:
            keys.add(String.format(KEY_CUSTOMERS_BY_NAME, tokens[1], tokens[2], tokens[3]));
            break;
        case QUERY_PAY_GET_CUST_PRE:
            keys.add(String.format(KEY_CUSTOMERID, tokens[1], tokens[2], tokens[3]));
            break;
        case QUERY_GET_CUST_C_DATA_PRE:
            keys.add(String.format(KEY_CUSTOMER_DATA, tokens[1], tokens[2], tokens[3]));
            break;           
            
        // Delivery
        case QUERY_GET_ORDER_ID_PRE:
            keys.add(String.format(KEY_NEW_ORDER_IDS, tokens[1], tokens[2]));
            break;
        case QUERY_DELIVERY_GET_CUST_ID_PRE:
            keys.add(String.format(KEY_CUSTOMERID_ORDER, tokens[1], tokens[2], tokens[3]));
            break;            
        case QUERY_GET_SUM_ORDER_AMOUNT_PRE:
            keys.add(String.format(KEY_OL_AMOUNT, tokens[1], tokens[2], tokens[3]));
            break;
          
        // OrderStatus
        case QUERY_ORDER_STAT_GET_ORDER_LINES_PRE:
            keys.add(String.format(KEY_ORDER_LINES, tokens[1], tokens[2], tokens[3]));
            break;              
        case QUERY_ORDER_STAT_GET_NEWEST_ORDER_PRE:
            keys.add(String.format(KEY_LAST_ORDER, tokens[1], tokens[2], tokens[3]));
            break;
            
        // StockLevel
        case QUERY_STOCK_GET_ITEM_IDS_PRE:
            keys.add(String.format(KEY_STOCK_LAST20ORDERS_ITEM_IDS, tokens[1], tokens[2]));
            break;
        case QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD_PRE:
            keys.add(String.format(KEY_STOCK_ITEMS_EQUAL_THRESHOLD, tokens[1], tokens[2]));
            break;
        }
        

        return keys;
    }

    @Override
    public Set<String> getImpactedKeysFromDml(String dml) {
        String[] tokens = dml.split(",");
        Set<String> set = new HashSet<>();
        switch (tokens[0]) {
        // new order
        case DML_UPDATE_DIST_PRE:
            set.add(String.format(KEY_DIST, tokens[1], tokens[2]));
            set.add(String.format(KEY_NEW_ORDER_IDS, tokens[1], tokens[2]));
            break;
        case DML_INSERT_OORDER_PRE:
            set.add(String.format(KEY_LAST_ORDER, tokens[1], tokens[2], tokens[3]));
            set.add(String.format(KEY_CUSTOMERID_ORDER, tokens[1], tokens[2], tokens[4]));
            set.add(String.format(KEY_ORDER_LINES, tokens[1], tokens[2], tokens[4]));
            break;
        case DML_INSERT_ORDER_LINE_PRE:
            set.add(String.format(KEY_OL_AMOUNT, tokens[1], tokens[2], tokens[3]));
            set.add(String.format(KEY_STOCK_LAST20ORDERS_ITEM_IDS, tokens[1], tokens[2]));
            break;
        case DML_UPDATE_STOCK_PRE:
            set.add(String.format(KEY_STOCK, tokens[1], tokens[2]));
            
            if (!TPCCConfig.useRangeQC) {
                // old and new stock quantity
                int quantity = Integer.parseInt(tokens[3]);
                if (10 <= quantity && quantity <= 20) {
                    set.add(String.format(KEY_STOCK_ITEMS_EQUAL_THRESHOLD, tokens[1], tokens[3]));
                }
                quantity = Integer.parseInt(tokens[7]);
                if (10 <= quantity && quantity <= 20) {
                    set.add(String.format(KEY_STOCK_ITEMS_EQUAL_THRESHOLD, tokens[1], tokens[7]));
                }
            }
            break;
        // payment
        case DML_UPDATE_CUST_BAL_PRE:
            set.add(String.format(KEY_CUSTOMERID, tokens[1], tokens[2], tokens[3]));
            break;
        case DML_UPDATE_CUST_BAL_C_DATA_PRE:
            set.add(String.format(KEY_CUSTOMERID, tokens[1], tokens[2], tokens[3]));
            set.add(String.format(KEY_CUSTOMER_DATA, tokens[1], tokens[2], tokens[3]));
            break;
        // delivery:
        case DML_DELETE_NEW_ORDER_PRE:
            set.add(String.format(KEY_NEW_ORDER_IDS, tokens[1], tokens[2]));
            break;
        case DML_UPDATE_CARRIER_ID_PRE:
            set.add(String.format(KEY_LAST_ORDER, tokens[1], tokens[2], tokens[5]));
            break;
        case DML_UPDATE_DELIVERY_DATE_PRE:
            set.add(String.format(KEY_ORDER_LINES, tokens[1], tokens[2], tokens[3]));
            break;
        case DML_UPDATE_CUST_BAL_DELIVERY_CNT_PRE:
            set.add(String.format(KEY_CUSTOMERID, tokens[1], tokens[2], tokens[3]));
            break;
        }
        return set;
    }

    @Override
    public QueryResult queryDataStore(String query) throws Exception {
        String[] tokens = query.split(",");
        ResultSet rs = null;

        switch (tokens[0]) {
        case QUERY_DISTRICT_NEXT_ORDER_PRE:
            PreparedStatement stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);
            stockGetDistOrderId.setInt(1, Integer.parseInt(tokens[1]));
            stockGetDistOrderId.setInt(2, Integer.parseInt(tokens[2]));
            rs = stockGetDistOrderId.executeQuery();
            if (!rs.next())
                return null;
            int next_o_id = rs.getInt("D_NEXT_O_ID");
            float d_tax = rs.getFloat("D_TAX");
            rs.close();
            return new QueryGetDistResult(query, next_o_id, d_tax);
        case QUERY_GET_COUNT_STOCK_PRE:
            PreparedStatement stockGetCountStock= this.getPreparedStatement(conn, stockGetCountStockSQL);
            int w_id = Integer.parseInt(tokens[1]);
            int d_id = Integer.parseInt(tokens[2]);
//            incrStats(referencesOrderLines, w_id+"_"+d_id);
            
            int o_id = Integer.parseInt(tokens[3]);
            int threshold = Integer.parseInt(tokens[4]);
            stockGetCountStock.setInt(1, w_id);
            stockGetCountStock.setInt(2, d_id);
            stockGetCountStock.setInt(3, o_id);
            stockGetCountStock.setInt(4, o_id);
            stockGetCountStock.setInt(5, w_id);
            stockGetCountStock.setInt(6, threshold);
            rs = stockGetCountStock.executeQuery();
            if (!rs.next())
                return null;
            int stock_count = rs.getInt("STOCK_COUNT");
            rs.close();
            return new QueryGetStockCountResult(query, stock_count);
        case QUERY_PAY_GET_CUST_BY_NAME_PRE:
            ArrayList<Integer> customerIds = new ArrayList<>();

            PreparedStatement customerByName= this.getPreparedStatement(conn, customerByNameSQL);
            int c_w_id = Integer.parseInt(tokens[1]);
            int c_d_id = Integer.parseInt(tokens[2]);
            String c_last = tokens[3];
            customerByName.setInt(1, c_w_id);
            customerByName.setInt(2, c_d_id);
            customerByName.setString(3, c_last);
            rs = customerByName.executeQuery();
            while (rs.next()) {
                customerIds.add(rs.getInt("C_ID"));
            }
            rs.close();
            return new QueryGetCustomerByNameResult(query, customerIds);            
        case QUERY_PAY_GET_CUST_PRE:
            PreparedStatement payGetCust =this.getPreparedStatement(conn, payGetCustSQL);
            c_w_id = Integer.parseInt(tokens[1]);
            c_d_id = Integer.parseInt(tokens[2]);
            int c_id = Integer.parseInt(tokens[3]);                 
            payGetCust.setInt(1, c_w_id);
            payGetCust.setInt(2, c_d_id);
            payGetCust.setInt(3, c_id);
            rs = payGetCust.executeQuery();            
            if (rs.next()) {
                Customer c = TPCCUtil.newCustomerFromResults(rs);
                c.c_id = c_id;
                c.c_last = rs.getString("C_LAST");
                c.c_first = rs.getString("C_FIRST");
                c.c_middle = rs.getString("C_MIDDLE");
                c.c_street_1 = rs.getString("C_STREET_1");
                c.c_street_2 = rs.getString("C_STREET_2");
                c.c_city = rs.getString("C_CITY");
                c.c_state = rs.getString("C_STATE");
                c.c_zip = rs.getString("C_ZIP");
                c.c_phone = rs.getString("C_PHONE");
                c.c_credit = rs.getString("C_CREDIT");
                c.c_credit_lim = rs.getFloat("C_CREDIT_LIM");
                c.c_discount = rs.getFloat("C_DISCOUNT");
                c.c_balance = rs.getFloat("C_BALANCE");
                c.c_ytd_payment = rs.getFloat("C_YTD_PAYMENT");
                c.c_payment_cnt = rs.getInt("C_PAYMENT_CNT");
                c.c_since = rs.getTimestamp("C_SINCE");
                rs.close();
                return new QueryGetCustomerById(query, c);
            }
            rs.close();
            return null;
        case QUERY_ORDER_STAT_GET_NEWEST_ORDER_PRE:
            PreparedStatement ordStatGetNewestOrd =this.getPreparedStatement(conn, ordStatGetNewestOrdSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
//            incrStats(referencesOOrder, w_id+"_"+d_id);
            
            c_id = Integer.parseInt(tokens[3]);
            ordStatGetNewestOrd.setInt(1, w_id);
            ordStatGetNewestOrd.setInt(2, d_id);
            ordStatGetNewestOrd.setInt(3, c_id);
            rs = ordStatGetNewestOrd.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException("No orders for O_W_ID=" + w_id
                        + " O_D_ID=" + d_id + " O_C_ID=" + c_id);
            }
            o_id = rs.getInt("O_ID");
            int o_carrier_id = rs.getInt("O_CARRIER_ID");
            Timestamp end_date = rs.getTimestamp("O_ENTRY_D");
            rs.close();
            return new QueryOrdStatGetNewestOrdResult(query, o_id, o_carrier_id, end_date);
        case QUERY_ORDER_STAT_GET_ORDER_LINES_PRE:
            PreparedStatement ordStatGetOrderLines=this.getPreparedStatement(conn, ordStatGetOrderLinesSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
//            incrStats(referencesOrderLines, w_id+"_"+d_id);
            
            o_id = Integer.parseInt(tokens[3]);
            ordStatGetOrderLines.setInt(1, o_id);
            ordStatGetOrderLines.setInt(2, d_id);
            ordStatGetOrderLines.setInt(3, w_id);
            rs = ordStatGetOrderLines.executeQuery();
            int orderlinesCount=0;
            long deliveryDate=-1;
            while (rs.next()) {
                orderlinesCount++;
                if (rs.getTimestamp("OL_DELIVERY_D") != null){
                    deliveryDate=rs.getTimestamp("ol_delivery_d").getTime();
                } else{
                    deliveryDate=-999;
                }
            }
            rs.close();
            return new QueryOrderStatGetOrderLinesResult(query, orderlinesCount, deliveryDate);
        case QUERY_GET_CUST_WHSE_PRE:
            PreparedStatement stmtGetCustWhse = this.getPreparedStatement(conn, stmtGetCustWhseSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
            c_id = Integer.parseInt(tokens[3]);
            stmtGetCustWhse.setInt(1, w_id);
            stmtGetCustWhse.setInt(2, w_id);
            stmtGetCustWhse.setInt(3, d_id);
            stmtGetCustWhse.setInt(4, c_id);
            rs = stmtGetCustWhse.executeQuery();
            if (!rs.next())
                throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
            float c_discount = rs.getFloat("C_DISCOUNT");
            c_last = rs.getString("C_LAST");
            String c_credit = rs.getString("C_CREDIT");
            float w_tax = rs.getFloat("W_TAX");
            rs.close();
            return new QueryGetCustWhseResult(query, c_discount, c_last, c_credit, w_tax);
//        case QUERY_GET_DIST_PRE:
//            PreparedStatement stmtGetDist = this.getPreparedStatement(conn, stmtGetDistSQL);
//            w_id = Integer.parseInt(tokens[1]);
//            d_id = Integer.parseInt(tokens[2]);
//            stmtGetDist.setInt(1, w_id);
//            stmtGetDist.setInt(2, d_id);
//            rs = stmtGetDist.executeQuery();
//            if (!rs.next()) {
//                throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
//            }
//            int d_next_o_id = rs.getInt("D_NEXT_O_ID");
//            d_tax = rs.getFloat("D_TAX");
//            rs.close();
//            return new QueryGetDistResult(query, d_next_o_id, d_tax);
        case QUERY_GET_ITEM_PRE:
            PreparedStatement stmtGetItem = this.getPreparedStatement(conn, stmtGetItemSQL);
            int ol_i_id = Integer.parseInt(tokens[1]);
            stmtGetItem.setInt(1, ol_i_id);
            rs = stmtGetItem.executeQuery();
            if (!rs.next()) {
                rs.close();
                return null;
            } else {
                float i_price = rs.getFloat("I_PRICE");
                String i_name = rs.getString("I_NAME");
                String i_data = rs.getString("I_DATA");
                rs.close();
                rs = null;
                return new QueryGetItemResult(query, i_price, i_name, i_data);
            }
        case QUERY_GET_STOCK_PRE:
            PreparedStatement stmtGetStock = this.getPreparedStatement(conn, stmtGetStockSQL);
            int ol_supply_w_id = Integer.parseInt(tokens[1]);
            ol_i_id = Integer.parseInt(tokens[2]); 
            stmtGetStock.setInt(1, ol_i_id);
            stmtGetStock.setInt(2, ol_supply_w_id);
            rs = stmtGetStock.executeQuery();
            if (!rs.next())
                throw new UserAbortException("I_ID=" + ol_i_id + " not found!");
            int s_quantity = rs.getInt("S_QUANTITY");
            String s_data = rs.getString("S_DATA");
            String s_dist_01 = rs.getString("S_DIST_01");
            String s_dist_02 = rs.getString("S_DIST_02");
            String s_dist_03 = rs.getString("S_DIST_03");
            String s_dist_04 = rs.getString("S_DIST_04");
            String s_dist_05 = rs.getString("S_DIST_05");
            String s_dist_06 = rs.getString("S_DIST_06");
            String s_dist_07 = rs.getString("S_DIST_07");
            String s_dist_08 = rs.getString("S_DIST_08");
            String s_dist_09 = rs.getString("S_DIST_09");
            String s_dist_10 = rs.getString("S_DIST_10");
            rs.close();
            rs = null;
            return new QueryGetStockResult(query, s_quantity, s_data, s_dist_01, s_dist_02,s_dist_03,s_dist_04,s_dist_05,
                    s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10);
        case QUERY_GET_WHSE_PRE:
            PreparedStatement payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL);
            w_id = Integer.parseInt(tokens[1]);
            payGetWhse.setInt(1, w_id);
            rs = payGetWhse.executeQuery();
            if (!rs.next())
                throw new RuntimeException("W_ID=" + w_id + " not found!");
            String w_street_1 = rs.getString("W_STREET_1");
            String w_street_2 = rs.getString("W_STREET_2");
            String w_city = rs.getString("W_CITY");
            String w_state = rs.getString("W_STATE");
            String w_zip = rs.getString("W_ZIP");
            String w_name = rs.getString("W_NAME");
            rs.close();
            rs = null;
            return new QueryGetWhseResult(query, w_street_1, w_street_2, w_city, w_state, w_zip, w_name);
        case QUERY_GET_DIST2_PRE:
            PreparedStatement payGetDist = this.getPreparedStatement(conn, payGetDistSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
            payGetDist.setInt(1, w_id);
            payGetDist.setInt(2, d_id);
            rs = payGetDist.executeQuery();
            if (!rs.next())
                throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
            String d_street_1 = rs.getString("D_STREET_1");
            String d_street_2 = rs.getString("D_STREET_2");
            String d_city = rs.getString("D_CITY");
            String d_state = rs.getString("D_STATE");
            String d_zip = rs.getString("D_ZIP");
            String d_name = rs.getString("D_NAME");
            rs.close();
            rs = null;
            return new QueryGetDist2Result(query, d_street_1, d_street_2, d_city, d_state, d_zip, d_name);
        case QUERY_GET_CUST_C_DATA_PRE:
            PreparedStatement payGetCustCdata = this.getPreparedStatement(conn, payGetCustCdataSQL);
            c_w_id = Integer.parseInt(tokens[1]);
            c_d_id = Integer.parseInt(tokens[2]);
            c_id = Integer.parseInt(tokens[3]);
            payGetCustCdata.setInt(1, c_w_id);
            payGetCustCdata.setInt(2, c_d_id);
            payGetCustCdata.setInt(3, c_id);
            rs = payGetCustCdata.executeQuery();
            if (!rs.next())
                throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");
            String c_data = rs.getString("C_DATA");
            rs.close();
            rs = null;
            return new QueryGetCustCDataResult(query, c_data);
        case QUERY_GET_ORDER_ID_PRE:
            PreparedStatement delivGetOrderId = this.getPreparedStatement(conn, delivGetOrderIdSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
//            incrStats(referencesNewOrder, w_id+"_"+d_id);
            
            delivGetOrderId.setInt(1, d_id);
            delivGetOrderId.setInt(2, w_id);
            rs = delivGetOrderId.executeQuery();
            
            List<Integer> newOrderIds = new ArrayList<>();
            while (rs.next()) {
                newOrderIds.add(rs.getInt(1));
            }
            return new QueryGetOrderIdResult(query, newOrderIds);            
        case QUERY_DELIVERY_GET_CUST_ID_PRE:
            PreparedStatement delivGetCustId = this.getPreparedStatement(conn, delivGetCustIdSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
//            incrStats(referencesOOrder, w_id+"_"+d_id);
            
            int no_o_id = Integer.parseInt(tokens[3]);
            delivGetCustId.setInt(1, no_o_id);
            delivGetCustId.setInt(2, d_id);
            delivGetCustId.setInt(3, w_id);
            rs = delivGetCustId.executeQuery();

            if (!rs.next())
                return null;
            c_id = rs.getInt("O_C_ID");
            rs.close();
            rs = null;
            return new QueryGetCustIdResult(query, c_id);
        case QUERY_GET_SUM_ORDER_AMOUNT_PRE:
            PreparedStatement delivSumOrderAmount = this.getPreparedStatement(conn, delivSumOrderAmountSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
//            incrStats(referencesOrderLines, w_id+"_"+d_id);
            
            no_o_id = Integer.parseInt(tokens[3]);
            delivSumOrderAmount.setInt(1, no_o_id);
            delivSumOrderAmount.setInt(2, d_id);
            delivSumOrderAmount.setInt(3, w_id);
            rs = delivSumOrderAmount.executeQuery();

            if (!rs.next())
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
            double ol_total = rs.getBigDecimal("OL_TOTAL").doubleValue();
            rs.close();
            rs = null;
            return new QueryGetSumOrderAmountResult(query, ol_total);
        case QUERY_STOCK_GET_ITEM_IDS_PRE:
            PreparedStatement stockGetItemIds = this.getPreparedStatement(conn, stockGetItemIdsSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
            o_id = Integer.parseInt(tokens[3]);
            stockGetItemIds.setInt(1, o_id);
            stockGetItemIds.setInt(2, o_id);
            stockGetItemIds.setInt(3, d_id);
            stockGetItemIds.setInt(4, w_id);
            rs = stockGetItemIds.executeQuery();
            Map<Integer, Set<Integer>> itemIds = new HashMap<>();
            while (rs.next()) {
                int ol_o_id = rs.getInt(1);
                ol_i_id = rs.getInt(2);
                Set<Integer> set = itemIds.get(ol_o_id);
                if (set == null) {
                    set = new HashSet<>();
                    itemIds.put(ol_o_id, set);
                }
                set.add(ol_i_id);
            }
            rs.close();
            rs = null;            
            return new QueryStockGetItemIDs(query, itemIds);
        case QUERY_STOCK_COUNT_ITEMS_IDS_PRE:
            PreparedStatement stockCountItemsIds = this.getPreparedStatement(conn, stockCountItemsIdsSQL);
            w_id = Integer.parseInt(tokens[1]);  
            threshold = Integer.parseInt(tokens[2]);
            int cnt = 1;
            int stockCount = 0;
            for (int i = 3; i < tokens.length; ++i) {
                if (cnt == 1) {
                    stockCountItemsIds.setInt(1, w_id);
                }
                if (cnt < SQL_INGROUP) {
                    stockCountItemsIds.setInt(++cnt, Integer.parseInt(tokens[i]));
                    if (cnt == SQL_INGROUP) {
                        stockCountItemsIds.setInt(1+SQL_INGROUP, threshold);                        
                        rs = stockCountItemsIds.executeQuery();
                        rs.next();
                        stockCount += rs.getBigDecimal(1).intValue();
                        rs.close();
                        rs = null;
                        
                        cnt = 1;
                    }
                }
            }
            if (cnt > 1) {
                int x = Integer.parseInt(tokens[3]);
                
                // fill the remaining with same value
                while (cnt < SQL_INGROUP) {
                    stockCountItemsIds.setInt(++cnt, x);
                    
                }
                stockCountItemsIds.setInt(1+SQL_INGROUP, threshold);
                rs = stockCountItemsIds.executeQuery();
                rs.next();
                stockCount += rs.getBigDecimal(1).intValue();
                rs.close();
                rs = null;
            }
            return new QueryStockGetCountItems(query, stockCount);
        case QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD_PRE:
            PreparedStatement stockItemsIdsEqualThreshold = this.getPreparedStatement(conn, stockItemsIdsEqualThresholdSQL);
            w_id = Integer.parseInt(tokens[1]);  
            threshold = Integer.parseInt(tokens[2]);
            stockItemsIdsEqualThreshold.setInt(1, w_id);
            stockItemsIdsEqualThreshold.setInt(2, threshold);
            rs = stockItemsIdsEqualThreshold.executeQuery();
            List<Integer> ids = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt(1));
            }
            rs.close();
            rs = null;
            return new QueryStockItemsEqualThreshold(query, ids);
        }       

        return null;
    }

    @Override
    public Set<CacheEntry> computeCacheEntries(String query, QueryResult result) {
        Set<CacheEntry> entries = new HashSet<>();
        String[] tokens = query.split(",");
        Map<String, String> map = new HashMap<>();
        String key = null;
        CacheEntry e = null;
        
        switch (tokens[0]) {
        // stock level
        case QUERY_DISTRICT_NEXT_ORDER_PRE:            
            QueryGetDistResult res = (QueryGetDistResult)result;
            map.put("next_o_id", String.valueOf(res.getNextOrderId()));
            map.put("d_tax", String.valueOf(res.getTax()));
            key = String.format(KEY_DIST, tokens[1], tokens[2]);
            break;
//        case QUERY_GET_DIST_PRE:
//            res = (QueryGetDistResult)result;
//            map.put("next_o_id", String.valueOf(res.getNextOrderId()));
//            map.put("d_tax", String.valueOf(res.getTax()));
//            key = String.format(KEY_DIST, tokens[1], tokens[2]);
//            break;
        case QUERY_GET_COUNT_STOCK_PRE:            
            break;
            // order status
        case QUERY_PAY_GET_CUST_BY_NAME_PRE:
            QueryGetCustomerByNameResult res19 = (QueryGetCustomerByNameResult) result;
            List<Integer> custIds = res19.getCustomerIds();
            e = new CacheEntry(String.format(KEY_CUSTOMERS_BY_NAME, tokens[1], tokens[2], tokens[3]), custIds, false);
            break;
        case QUERY_PAY_GET_CUST_PRE:
            QueryGetCustomerById res2 = (QueryGetCustomerById)result;
            Customer c = res2.getCustomer();
            map.put("c_id", String.valueOf(c.c_id));
            map.put("c_last", c.c_last);
            map.put("c_first", c.c_first);
            map.put("c_middle", c.c_middle);
            map.put("c_street_1", c.c_street_1);
            map.put("c_street_2", c.c_street_2);
            map.put("c_city", c.c_city);
            map.put("c_state", c.c_state);
            map.put("c_zip", c.c_zip);
            map.put("c_phone", c.c_phone);
            map.put("c_credit", c.c_credit);
            map.put("c_credit_lim", String.valueOf(c.c_credit_lim));
            map.put("c_discount", String.valueOf(c.c_discount));
            map.put("c_balance", String.valueOf(c.c_balance));
            map.put("c_ytd_payment", String.valueOf(c.c_ytd_payment));
            map.put("c_payment_cnt", String.valueOf(c.c_payment_cnt));
            map.put("c_since", String.valueOf(c.c_since.getTime()));      
            key = String.format(KEY_CUSTOMERID, tokens[1], tokens[2], tokens[3]);
            break;
        case QUERY_GET_CUST_C_DATA_PRE:
            QueryGetCustCDataResult res12 = (QueryGetCustCDataResult) result;
            map.put("c_data", res12.getCData());
            key = String.format(KEY_CUSTOMER_DATA, tokens[1], tokens[2], tokens[3]);
            break;
        case QUERY_ORDER_STAT_GET_NEWEST_ORDER_PRE:
            QueryOrdStatGetNewestOrdResult res3 = (QueryOrdStatGetNewestOrdResult) result;
            map.put("o_id", String.valueOf(res3.getOrderId()));
            map.put("o_carrier_id", String.valueOf(res3.getCarrierId()));
            map.put("o_delivery_date", String.valueOf(res3.getTimestamp().getTime()));
            key = String.format(KEY_LAST_ORDER, tokens[1], tokens[2], tokens[3]);
            break;
        case QUERY_ORDER_STAT_GET_ORDER_LINES_PRE:
            QueryOrderStatGetOrderLinesResult res13 = (QueryOrderStatGetOrderLinesResult)result;
            map.put("dev_date", String.valueOf(res13.getDeliveryDate()));
            map.put("ol_cnt", String.valueOf(res13.getOrderLinesCount()));
            key = String.format(KEY_ORDER_LINES, tokens[1], tokens[2], tokens[3]);
            break;
            // new order
        case QUERY_GET_CUST_WHSE_PRE:
            QueryGetCustWhseResult res4 = (QueryGetCustWhseResult)result;
            map.put("c_discount", String.valueOf(res4.getDiscount()));
            map.put("c_last", res4.getLast());
            map.put("c_credit", res4.getCredit());
            map.put("w_tax", String.valueOf(res4.getTax()));
            key = String.format(KEY_CUSTWHSE, tokens[1], tokens[2], tokens[3]);
            break;
        case QUERY_GET_ITEM_PRE:
            QueryGetItemResult res5 = (QueryGetItemResult)result;
            map.put("i_price", String.valueOf(res5.getPrice()));
            map.put("i_name", res5.getName());
            map.put("i_data", res5.getData());
            key = String.format(KEY_ITEM, tokens[1]);
            break;
        case QUERY_GET_STOCK_PRE:
            QueryGetStockResult res6 = (QueryGetStockResult)result;
            map.put("s_quantity", String.valueOf(res6.getQuantity()));
            map.put("s_data", res6.getData());
            map.put("s_dist_01",res6.getDist01());
            map.put("s_dist_02",res6.getDist02());
            map.put("s_dist_03",res6.getDist03());
            map.put("s_dist_04",res6.getDist04());
            map.put("s_dist_05",res6.getDist05());
            map.put("s_dist_06",res6.getDist06());
            map.put("s_dist_07",res6.getDist07());
            map.put("s_dist_08",res6.getDist08());
            map.put("s_dist_09",res6.getDist09());
            map.put("s_dist_10",res6.getDist10());
            key = String.format(KEY_STOCK, tokens[1], tokens[2]);
            break;
        case QUERY_GET_WHSE_PRE:
            QueryGetWhseResult res7 = (QueryGetWhseResult)result;
            map.put("w_street_1", res7.getStreet1());
            map.put("w_street_2", res7.getStreet2());
            map.put("w_city", res7.getCity());
            map.put("w_state", res7.getState());
            map.put("w_zip", res7.getZip());
            map.put("w_name", res7.getName());
            key = String.format(KEY_WAREHOUSE, tokens[1]);
            break;
        case QUERY_GET_DIST2_PRE:
            QueryGetDist2Result res8 = (QueryGetDist2Result)result;
            map.put("d_street_1", res8.getStreet1());
            map.put("d_street_2", res8.getStreet2());
            map.put("d_city", res8.getCity());
            map.put("d_state", res8.getState());
            map.put("d_zip", res8.getZip());
            map.put("d_name", res8.getName());
            key = String.format(KEY_DIST2, tokens[1], tokens[2]);
            break;
//        case QUERY_GET_ORDER_ID_PRE:
//            QueryGetOrderIdResult res9 = (QueryGetOrderIdResult) result;
//            map.put("no_o_id", String.valueOf(res9.getNoOId()));
//            break;
        case QUERY_DELIVERY_GET_CUST_ID_PRE:
            QueryGetCustIdResult res10 = (QueryGetCustIdResult) result;
            map.put("c_id", String.valueOf(res10.getOCId()));
            key = String.format(KEY_CUSTOMERID_ORDER, tokens[1], tokens[2], tokens[3]);
            break;
        case QUERY_GET_SUM_ORDER_AMOUNT_PRE:
            QueryGetSumOrderAmountResult res11 = (QueryGetSumOrderAmountResult) result;
            map.put("ol_amount", String.valueOf(res11.getTotal()));
            key = String.format(KEY_OL_AMOUNT, tokens[1], tokens[2], tokens[3]);
            break;
        case QUERY_GET_ORDER_ID_PRE:
            QueryGetOrderIdResult res14 = (QueryGetOrderIdResult) result;
            List<Integer> ids = res14.getNewOrderIds();            
            e = new CacheEntry(String.format(KEY_NEW_ORDER_IDS, tokens[1], tokens[2]), ids, false);            
            break;
        case QUERY_STOCK_GET_ITEM_IDS_PRE:
            QueryStockGetItemIDs res15 = (QueryStockGetItemIDs) result;
            Map<Integer, Set<Integer>> itemIds = res15.getItemIDs();
            e = new CacheEntry(String.format(KEY_STOCK_LAST20ORDERS_ITEM_IDS, tokens[1], tokens[2]), itemIds, false);
            break;
        case QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD_PRE:
            QueryStockItemsEqualThreshold res16 = (QueryStockItemsEqualThreshold) result;
            ids = res16.getIds();
            e = new CacheEntry(String.format(KEY_STOCK_ITEMS_EQUAL_THRESHOLD, tokens[1], tokens[2]), ids, false);
            break;
        }

        if (map.size() > 0) {
            e = new CacheEntry(key, map, false);
        }
        
        if (e != null) entries.add(e);

        return entries;
    }

    @Override
    public boolean dmlDataStore(String dml) throws Exception {
        String[] tokens = dml.split(",");
        switch (tokens[0]) {
        // new order
        case DML_UPDATE_DIST_PRE:
            PreparedStatement stmtUpdateDist = this.getPreparedStatement(conn, stmtUpdateDistSQL);
            int w_id = Integer.parseInt(tokens[1]);
            int d_id = Integer.parseInt(tokens[2]);
            stmtUpdateDist.setInt(1, w_id);
            stmtUpdateDist.setInt(2, d_id);
            int result = stmtUpdateDist.executeUpdate();
            if (result == 0)
                throw new RuntimeException("Error!! Cannot update next_order_id on district for D_ID=" + d_id + " D_W_ID=" + w_id);
            return true;
        case DML_UPDATE_STOCK_PRE:
            PreparedStatement stmtUpdateStock = this.getPreparedStatement(conn, stmtUpdateStockSQL);
            int ol_supply_w_id = Integer.parseInt(tokens[1]);
            int ol_i_id = Integer.parseInt(tokens[2]);
            //int s_quantity = Integer.parseInt(tokens[3]);
            int ol_quantity = Integer.parseInt(tokens[4]);
            int s_remote_cnt_increment = Integer.parseInt(tokens[6]);
            stmtUpdateStock.setInt(1, ol_quantity);
            stmtUpdateStock.setInt(2, ol_quantity);
            stmtUpdateStock.setInt(3, ol_quantity);
            stmtUpdateStock.setInt(4, ol_quantity);
            stmtUpdateStock.setInt(5, s_remote_cnt_increment);
            stmtUpdateStock.setInt(6, ol_i_id);
            stmtUpdateStock.setInt(7, ol_supply_w_id);
            result = stmtUpdateStock.executeUpdate();
            if (result == 0)
                throw new RuntimeException("Error!! Cannot update Stock for S_W_ID=" + ol_supply_w_id + " S_I_ID=" + ol_i_id);
            return true;
        case DML_INSERT_OORDER_PRE:
            PreparedStatement stmtInsertOOrder = this.getPreparedStatement(conn, stmtInsertOOrderSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
//            incrStats(insertsOOrder, w_id+"_"+d_id);
            
            int c_id = Integer.parseInt(tokens[3]);
            int o_id = Integer.parseInt(tokens[4]);
            long timestamp = Long.parseLong(tokens[5]);
            int o_ol_cnt =  Integer.parseInt(tokens[6]);
            int o_all_local =  Integer.parseInt(tokens[7]);
            
            stmtInsertOOrder.setInt(1, o_id);
            stmtInsertOOrder.setInt(2, d_id);
            stmtInsertOOrder.setInt(3, w_id);
            stmtInsertOOrder.setInt(4, c_id);
            stmtInsertOOrder.setTimestamp(5, new Timestamp(timestamp));
            stmtInsertOOrder.setInt(6, o_ol_cnt);
            stmtInsertOOrder.setInt(7, o_all_local);
            stmtInsertOOrder.executeUpdate();
            return true;
        case DML_INSERT_NEW_ORDER_PRE:
            PreparedStatement stmtInsertNewOrder = this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
//            incrStats(insertsNewOrder, w_id+"_"+d_id);
            
            o_id = Integer.parseInt(tokens[3]);
            stmtInsertNewOrder.setInt(1, o_id);
            stmtInsertNewOrder.setInt(2, d_id);
            stmtInsertNewOrder.setInt(3, w_id);
            stmtInsertNewOrder.executeUpdate();
            return true;        
        case DML_INSERT_ORDER_LINE_PRE:
            PreparedStatement stmtInsertOrderLine = this.getPreparedStatement(conn, stmtInsertOrderLineSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
//            incrStats(insertsOrderLines, w_id+"_"+d_id);
            
            o_id = Integer.parseInt(tokens[3]);
            int ol_number = Integer.parseInt(tokens[4]);
            ol_i_id = Integer.parseInt(tokens[5]);
            ol_supply_w_id = Integer.parseInt(tokens[6]);
            ol_quantity = Integer.parseInt(tokens[7]);
            float ol_amount = Float.parseFloat(tokens[8]);
            String ol_dist_info = tokens[9];
            stmtInsertOrderLine.setInt(1, o_id);
            stmtInsertOrderLine.setInt(2, d_id);
            stmtInsertOrderLine.setInt(3, w_id);
            stmtInsertOrderLine.setInt(4, ol_number);
            stmtInsertOrderLine.setInt(5, ol_i_id);
            stmtInsertOrderLine.setInt(6, ol_supply_w_id);
            stmtInsertOrderLine.setInt(7, ol_quantity);
            stmtInsertOrderLine.setFloat(8, ol_amount);
            stmtInsertOrderLine.setString(9, ol_dist_info);
            stmtInsertOrderLine.executeUpdate();
            return true;
        // payment
        case DML_UPDATE_WHSE_PRE:
            PreparedStatement payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL);
            w_id = Integer.parseInt(tokens[1]);
            float h_amount = Float.parseFloat(tokens[2]);
            payUpdateWhse.setFloat(1, h_amount);
            payUpdateWhse.setInt(2, w_id);
            // MySQL reports deadlocks due to lock upgrades:
            // t1: read w_id = x; t2: update w_id = x; t1 update w_id = x
            result = payUpdateWhse.executeUpdate();
            return (result == 1);
        case DML_PAY_UPDATE_DIST_PRE:
            PreparedStatement payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
            h_amount = Float.parseFloat(tokens[3]);
            payUpdateDist.setFloat(1, h_amount);
            payUpdateDist.setInt(2, w_id);
            payUpdateDist.setInt(3, d_id);
            result = payUpdateDist.executeUpdate();
            return (result != 0);
        case DML_UPDATE_CUST_BAL_PRE:
            PreparedStatement payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL);
            int c_w_id = Integer.parseInt(tokens[1]);
            int c_d_id = Integer.parseInt(tokens[2]);
            c_id = Integer.parseInt(tokens[3]);
            String c_balance = tokens[4];
            float c_ytd_payment = Float.parseFloat(tokens[5]);
            int c_payment_cnt = Integer.parseInt(tokens[6]);
            payUpdateCustBal.setBigDecimal(1, new BigDecimal(c_balance));
            payUpdateCustBal.setFloat(2, c_ytd_payment);
            payUpdateCustBal.setInt(3, c_payment_cnt);
            payUpdateCustBal.setInt(4, c_w_id);
            payUpdateCustBal.setInt(5, c_d_id);
            payUpdateCustBal.setInt(6, c_id);
            result = payUpdateCustBal.executeUpdate();
            return (result != 0);
        case DML_UPDATE_CUST_BAL_C_DATA_PRE:
            PreparedStatement payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL);
            c_w_id = Integer.parseInt(tokens[1]);
            c_d_id = Integer.parseInt(tokens[2]);
            c_id = Integer.parseInt(tokens[3]);
            c_balance = tokens[4];
            c_ytd_payment = Float.parseFloat(tokens[5]);
            c_payment_cnt = Integer.parseInt(tokens[6]);
            String c_data = tokens[7];
            payUpdateCustBalCdata.setBigDecimal(1, new BigDecimal(c_balance));
            payUpdateCustBalCdata.setFloat(2, c_ytd_payment);
            payUpdateCustBalCdata.setInt(3, c_payment_cnt);
            payUpdateCustBalCdata.setString(4, c_data);
            payUpdateCustBalCdata.setInt(5, c_w_id);
            payUpdateCustBalCdata.setInt(6, c_d_id);
            payUpdateCustBalCdata.setInt(7, c_id);
            result = payUpdateCustBalCdata.executeUpdate();
            return (result != 0);
        case DML_INSERT_HISTORY_PRE:
            PreparedStatement payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL);
            c_w_id = Integer.parseInt(tokens[1]);
            c_d_id = Integer.parseInt(tokens[2]);
            c_id = Integer.parseInt(tokens[3]);
            d_id = Integer.parseInt(tokens[4]);
            w_id = Integer.parseInt(tokens[5]);
            timestamp = Long.parseLong(tokens[6]);
            h_amount = Float.parseFloat(tokens[7]);
            String h_data = tokens[8];
            payInsertHist.setInt(1, c_d_id);
            payInsertHist.setInt(2, c_w_id);
            payInsertHist.setInt(3, c_id);
            payInsertHist.setInt(4, d_id);
            payInsertHist.setInt(5, w_id);
            payInsertHist.setTimestamp(6, new Timestamp(timestamp));
            payInsertHist.setFloat(7, h_amount);
            payInsertHist.setString(8, h_data);
            payInsertHist.executeUpdate();
            break;
        // delivery
        case DML_DELETE_NEW_ORDER_PRE:
            PreparedStatement delivDeleteNewOrder = this.getPreparedStatement(conn, delivDeleteNewOrderSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
            int no_o_id = Integer.parseInt(tokens[3]);
            delivDeleteNewOrder.setInt(1, no_o_id);
            delivDeleteNewOrder.setInt(2, d_id);
            delivDeleteNewOrder.setInt(3, w_id);
            result = delivDeleteNewOrder.executeUpdate();
            return (result == 1);
        case DML_UPDATE_CARRIER_ID_PRE:
            PreparedStatement delivUpdateCarrierId = this.getPreparedStatement(conn, delivUpdateCarrierIdSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
            no_o_id = Integer.parseInt(tokens[3]);
            int o_carrier_id = Integer.parseInt(tokens[4]);
            delivUpdateCarrierId.setInt(1, o_carrier_id);
            delivUpdateCarrierId.setInt(2, no_o_id);
            delivUpdateCarrierId.setInt(3, d_id);
            delivUpdateCarrierId.setInt(4, w_id);
            result = delivUpdateCarrierId.executeUpdate();
            return true;
        case DML_UPDATE_DELIVERY_DATE_PRE:
            PreparedStatement delivUpdateDeliveryDate = this.getPreparedStatement(conn, delivUpdateDeliveryDateSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
            no_o_id = Integer.parseInt(tokens[3]);
            long deliveryDate = Long.parseLong(tokens[4]);
            delivUpdateDeliveryDate.setTimestamp(1, new Timestamp(deliveryDate));
            delivUpdateDeliveryDate.setInt(2, no_o_id);
            delivUpdateDeliveryDate.setInt(3, d_id);
            delivUpdateDeliveryDate.setInt(4, w_id);
            result = delivUpdateDeliveryDate.executeUpdate();
            return true;
        case DML_UPDATE_CUST_BAL_DELIVERY_CNT_PRE:
            PreparedStatement delivUpdateCustBalDelivCnt = this.getPreparedStatement(conn, delivUpdateCustBalDelivCntSQL);
            w_id = Integer.parseInt(tokens[1]);
            d_id = Integer.parseInt(tokens[2]);
            c_id = Integer.parseInt(tokens[3]);
            String ol_total = tokens[4];
            BigDecimal bd = new BigDecimal(ol_total);
            delivUpdateCustBalDelivCnt.setBigDecimal(1, bd);
            delivUpdateCustBalDelivCnt.setInt(2, w_id);
            delivUpdateCustBalDelivCnt.setInt(3, d_id);
            delivUpdateCustBalDelivCnt.setInt(4, c_id);
            result = delivUpdateCustBalDelivCnt.executeUpdate();
            return true;
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CacheEntry applyDelta(Delta delta, CacheEntry cacheEntry) {
        Object cacheVal = cacheEntry.getValue();
        if (cacheVal instanceof HashMap) {
            HashMap map = (HashMap)cacheVal;
            if (map.size() == 0) {
                applyDeltaHashMap(delta, (HashMap<String, String>)cacheVal);    
            } else {
                Object akey = map.keySet().iterator().next();
                if (akey instanceof String) {
                    applyDeltaHashMap(delta, (HashMap<String, String>)cacheVal);
                } else if (akey instanceof Integer) {
                    applyDeltaHashMap2(delta, (HashMap<Integer, Set<Integer>>)cacheVal);
                }
            }
        } else if (cacheVal instanceof List) {
            List<Integer> val = (List<Integer>)cacheVal;
            Object obj = delta.getValue();
            if (obj instanceof Integer) {
                switch (delta.getType()) {
                case Delta.TYPE_RMW:
                    if (val.contains(obj)) {
                        val.remove((Integer)obj);
                    } else {
                        System.out.println("Value of key "+cacheEntry.getKey()+" does not contains "+obj);
                    }
                    break;
                case Delta.TYPE_APPEND:
                    if (val.contains((obj))) {
                        System.out.println("Value of key "+cacheEntry.getKey()+ " contains "+ obj);
                    } else {
                            val.add((Integer)obj);
                    }
                    break;
                }
            }
        }
        return cacheEntry;
    }

    private void applyDeltaHashMap2(Delta delta,
            HashMap<Integer, Set<Integer>> v) {
        String dVal = (String) delta.getValue();
        String[] ops = dVal.split(";");
        int old_o_id = 0;
        for (String op: ops) {
            String[] fields = op.split(",");
            switch (fields[0]) {
            case ADD:
                int o_id = Integer.parseInt(fields[2]);
                int i_id = Integer.parseInt(fields[4]);
                 Set<Integer> set = v.get(o_id);
                if (set == null) {
                    set = new HashSet<>();
                    v.put(o_id, set);
                }
                set.add(i_id);
                old_o_id = o_id - 20;
                break;
            case REMOVE_FIRST:
                v.remove(old_o_id);
                break;
            default:
                System.out.println("Error: not a delta of type ADD");
                break;
            }
        }
    }

    private void applyDeltaHashMap(Delta delta, HashMap<String, String> cacheVal) {
        String dVal = (String) delta.getValue();
        String[] ops = dVal.split(";");
        
        // handle special case where there must be a check on the list of attributes.
        if (dVal.contains(CHECK)) {
            for (String op: ops) {
                if (op.contains(CHECK)) {
                    String[] fields = op.split(",");
                    if (fields[0].equals(CHECK)) {
                        String attr = fields[1];
                        String val = cacheVal.get(attr);
                        if (val == null || !val.equals(fields[2])) {
                            return;
                        }
                    }
                }
            }
        }
        
        for (String op: ops) {
            String[] fields = op.split(",");
            switch (fields[0]) {
            case SET:
                cacheVal.put(fields[1], fields[2]);
                break;
            case INCR:
                String val = cacheVal.get(fields[1]);
                if (val != null) {
                    double d = Double.parseDouble(val);
                    double i = Double.parseDouble(fields[2]);
                    cacheVal.put(fields[1], String.valueOf(d+i));
                }
                break;
            case INCR_OR_SET:
                val = cacheVal.get(fields[1]);
                if (val == null) {
                    cacheVal.put(fields[1], fields[2]);
                } else {
                    double d = Double.parseDouble(val);
                    double i = Double.parseDouble(fields[2]);
                    cacheVal.put(fields[1], String.valueOf(d+i));
                }
            }
        }
    }

    @Override
    public byte[] serialize(CacheEntry cacheEntry) {
        Object val = cacheEntry.getValue();
        byte[] bytes = null;
        
        if (val instanceof HashMap) {
            HashMap map = (HashMap)val;
            if (map.size() == 0) {
                bytes = serializeHashMap((HashMap<String, String>)val);
            } else {
                Object akey = map.keySet().iterator().next();
                if (akey instanceof String) {
                    bytes = serializeHashMap((HashMap<String, String>)val);
                } else if (akey instanceof Integer) {
                    bytes = serializeHashMap2((HashMap<Integer, Set<Integer>>)val);
                }
            }
        } else if (val instanceof List) {
            bytes = serializeList((List<Integer>) val);
        }
        return bytes;
    }

    private byte[] serializeList(List<Integer> val) {
        if (val == null || val.size() == 0) return null;
        ByteBuffer bf = ByteBuffer.allocate(val.size()*4);
        for (Integer v: val) {
            bf.putInt(v);
        }
        return bf.array();
    }

    private byte[] serializeHashMap(Map<String, String> map) {
        int total = 0;
        for (String key: map.keySet()) {
            total += key.length()+4;
            String val = map.get(key);
            total += val.length()+4;
        }

        ByteBuffer buffer = ByteBuffer.allocate(total);
        for (String key: map.keySet()) {
            buffer.putInt(key.length());
            buffer.put(key.getBytes());
            String val = map.get(key);
            buffer.putInt(val.length());
            buffer.put(val.getBytes());            
        }
        return buffer.array();
    }
    
    private byte[] serializeHashMap2(Map<Integer, Set<Integer>> map) {
        int total = 0;
        for (Integer key: map.keySet()) {
            total += 4;
            total += 4; // size of set
            total += 4 * map.get(key).size();
        }

        ByteBuffer buffer = ByteBuffer.allocate(total);
        for (Integer key: map.keySet()) {
            buffer.putInt(key.intValue());
            
            Set<Integer> set = map.get(key);
            buffer.putInt(set.size());
            for (Integer x: set) {
                buffer.putInt(x);
            }
        }
        return buffer.array();
    }

    @Override
    public CacheEntry deserialize(String key, Object obj, byte[] buffer) {
        byte[] bytes = (byte[]) obj;
        
        int offset = 0;
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        
        if (key.startsWith(KEY_NEW_ORDER_IDS_PRE) || key.startsWith(KEY_CUSTOMERS_BY_NAME_PRE)) {
            List<Integer> list = new ArrayList<>();
            while (offset < bytes.length) {
                int o_id = buff.getInt();
                list.add(o_id);
                offset+=4;
            }
            return new CacheEntry(key, list, false);
        } else if (key.startsWith(KEY_STOCK_LAST20ORDERS_ITEM_IDS_PRE)) {
            Map<Integer, Set<Integer>> map = new HashMap<>();
            while (offset < bytes.length) {
                int o_id = buff.getInt();
                offset += 4;
                
                int size = buff.getInt();
                offset += 4;
                
                Set<Integer> set = new HashSet<>();
                for (int i = 0; i < size; ++i) {
                    set.add(buff.getInt());
                    offset += 4;
                }
                
                map.put(o_id, set);
            }
            return new CacheEntry(key, map, false);
        } else if (key.startsWith(KEY_STOCK_ITEMS_EQUAL_THRESHOLD_PRE)) {
            List<Integer> ids = new ArrayList<>();
            offset = 0;
            while (offset < bytes.length) {
                ids.add(buff.getInt());
                offset += 4;
            }
            return new CacheEntry(key, ids, false);
        }
        
        
        Map<String, String> map = new HashMap<>();
        while (offset < bytes.length) {
            int len = buff.getInt();
            offset += 4;

            byte[] bs = new byte[len];
            buff.get(bs);
            offset += len;            
            String k = new String(bs);

            len = buff.getInt();
            offset += 4;

            bs = new byte[len];
            buff.get(bs);
            String v = new String(bs);
            offset += len;

            map.put(k, v);
        }

        return new CacheEntry(key, map, false);
    }

    @Override
    public byte[] serialize(Delta delta) {
        byte[] bytes = null;
        
        if (delta.getType() == Delta.TYPE_APPEND) {
            int x = (int)delta.getValue();
            ByteBuffer bf = ByteBuffer.allocate(4);
            bf.putInt(x);
            bytes = bf.array();
        }
        
        if (delta.getType() == Delta.TYPE_RMW || delta.getType() == Delta.TYPE_SET) {
            Map<String, String> obj = new HashMap<>();
            String val = (String)delta.getValue();
            String[] fields = val.split(";");
            for (String field: fields) {
                String[] tokens = field.split(",");
                obj.put(tokens[1], tokens[2]);
            }        
            bytes = serializeHashMap(obj);
        }
        return bytes;
    }

    @Override
    public int getHashCode(String key) {
        // return the ware house
        if (false) {
            if (key.contains("_w") || key.contains("RANGE")) {
                String[] fs = key.split(",");
                try {
                    int x = Integer.parseInt(fs[1]);
    //                System.out.println(x);
                    return x-1;
                } catch (NumberFormatException e) {
                    System.out.println("Cannot get warehouse number " + key);
                }
            }
            
            if (key.contains("S-")) {   // this is a session key
                try {
                    int x = Integer.parseInt(key.split("-")[1]);
                    return x-1;
                } catch (NumberFormatException e) {
                    System.out.println("Cannot get warehouse number " + key);
                }
            }
            
    //        System.out.println("This key has no warehouse info "+ key);
            
            return -1;
        } else {
            return 0;
        }
        
//        int hc = cacheStore.getHashCode(key);
//        return hc % mc.getPool().getServers().length; 
//        
//        int idx = buffKey.lastIndexOf('_');
//        buffKey = buffKey.substring(0, idx);
//        int hc = buffKey.hashCode() & 0xfffffff;
//        return hc % mc.getPool().getServers().length;
    }

    @Override
    public QueryResult computeQueryResult(String query, Set<CacheEntry> entries) {
        String[] tokens = query.split(",");
        
        if (tokens[0].equals(QUERY_RANGE_GET_ITEMS_PRE)) {
            List<Integer> list = new ArrayList<>();
            for (CacheEntry entry: entries) {
                list.addAll((List<Integer>)entry.getValue());
            }
            return new QueryResultRangeGetItems(query, list);
        }
        
        // only one entry
        Set<String> keys = getReferencedKeysFromQuery(query);
        if (keys.size() == 0) return null;
        String key = keys.iterator().next();    // only one key
        CacheEntry entry = null;
        for (CacheEntry e: entries) {
            if (e.getKey().equals(key)) {
                entry = e;
                break;
            }
        }
        if (entry == null) return null;
        Map<String, String> map1  = null;
        Map<Integer, Set<Integer>> map2  = null;
        List<Integer> ids = null;
        if (entry.getValue() instanceof HashMap) {
            HashMap map = (HashMap)entry.getValue();
            Object akey = map.keySet().iterator().next();
            if (akey instanceof String) {
                map1 = (Map<String, String>)entry.getValue();
            } else if (akey instanceof Integer) {
                map2 = (Map<Integer, Set<Integer>>)entry.getValue();
            }
        }
        if (entry.getValue() instanceof List) {
            ids = (List<Integer>)entry.getValue();
        }
        
        switch (tokens[0]) {
        case QUERY_DISTRICT_NEXT_ORDER_PRE:
            int next_o_id = (int)Float.parseFloat(map1.get("next_o_id"));
            float d_tax = Float.parseFloat(map1.get("d_tax"));
            return new QueryGetDistResult(query, next_o_id, d_tax);
//        case QUERY_GET_DIST_PRE:
//            next_o_id = (int)Float.parseFloat(map.get("next_o_id"));
//            d_tax = Float.parseFloat(map.get("d_tax"));
//            return new QueryGetDistResult(query, next_o_id, d_tax);
        case QUERY_PAY_GET_CUST_PRE:
            int c_id = Integer.parseInt(map1.get("c_id"));
            String c_last = map1.get("c_last");
            Customer c = new Customer();
            c.c_id = c_id;
            c.c_last = c_last;
            c.c_first = map1.get("c_first");
            c.c_middle = map1.get("c_middle");
            c.c_street_1 = map1.get("c_street_1");
            c.c_street_2 = map1.get("c_street_2");
            c.c_city = map1.get("c_city");
            c.c_state = map1.get("c_state");
            c.c_zip = map1.get("c_zip");
            c.c_phone = map1.get("c_phone");
            c.c_credit = map1.get("c_credit");
            c.c_credit_lim = Float.parseFloat(map1.get("c_credit_lim"));
            c.c_discount = Float.parseFloat(map1.get("c_discount"));
            c.c_balance = Float.parseFloat(map1.get("c_balance"));
            c.c_ytd_payment = Float.parseFloat(map1.get("c_ytd_payment"));
            c.c_payment_cnt = Integer.parseInt(map1.get("c_payment_cnt"));
            c.c_since = new Timestamp(Long.parseLong(map1.get("c_since")));
            return new QueryGetCustomerById(query,c);
        case QUERY_GET_CUST_C_DATA_PRE:
            String c_data = map1.get("c_data");
            return new QueryGetCustCDataResult(query, c_data);
        case QUERY_ORDER_STAT_GET_NEWEST_ORDER_PRE:
            int o_id = Integer.parseInt(map1.get("o_id"));
            int o_carrier_id = Integer.parseInt(map1.get("o_carrier_id"));
            long deliveryDate = Long.parseLong(map1.get("o_delivery_date"));
            return new QueryOrdStatGetNewestOrdResult(query, o_id, o_carrier_id, new Timestamp(deliveryDate));
        case QUERY_ORDER_STAT_GET_ORDER_LINES_PRE:
            long dev_date = Long.parseLong(map1.get("dev_date"));
            int ol_cnt = Integer.parseInt(map1.get("ol_cnt"));
            return new QueryOrderStatGetOrderLinesResult(query, ol_cnt, dev_date);
        case QUERY_GET_CUST_WHSE_PRE:
            float c_discount = Float.parseFloat(map1.get("c_discount"));
            c_last = map1.get("c_last");
            String c_credit = map1.get("c_credit");
            float w_tax = Float.parseFloat(map1.get("w_tax"));
            return new QueryGetCustWhseResult(query, c_discount, c_last, c_credit, w_tax);    
        case QUERY_GET_ITEM_PRE:
            float i_price = Float.parseFloat(map1.get("i_price"));
            String i_name = map1.get("i_name");
            String i_data = map1.get("i_data");
            return new QueryGetItemResult(query, i_price, i_name, i_data);
        case QUERY_GET_STOCK_PRE:
            int s_quantity = Integer.parseInt(map1.get("s_quantity"));
            String s_data = map1.get("s_data");
            String s_dist_01 = map1.get("s_dist_01");
            String s_dist_02 = map1.get("s_dist_02");
            String s_dist_03 = map1.get("s_dist_03");
            String s_dist_04 = map1.get("s_dist_04");
            String s_dist_05 = map1.get("s_dist_05");
            String s_dist_06 = map1.get("s_dist_06");
            String s_dist_07 = map1.get("s_dist_07");
            String s_dist_08 = map1.get("s_dist_08");
            String s_dist_09 = map1.get("s_dist_09");
            String s_dist_10 = map1.get("s_dist_10");
            return new QueryGetStockResult(query, s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, 
                    s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10);
        case QUERY_GET_WHSE_PRE:
            String w_street_1 = map1.get("w_street_1");
            String w_street_2 = map1.get("w_street_2");
            String w_city = map1.get("w_city");
            String w_state = map1.get("w_state");
            String w_zip = map1.get("w_zip");
            String w_name = map1.get("w_name");
            return new QueryGetWhseResult(query, w_street_1, w_street_2, w_city, w_state, w_zip, w_name);
        case QUERY_GET_DIST2_PRE:
            String d_street_1 = map1.get("d_street_1");
            String d_street_2 = map1.get("d_street_2");
            String d_city = map1.get("d_city");
            String d_state = map1.get("d_state");
            String d_zip = map1.get("d_zip");
            String d_name = map1.get("d_name");
            return new QueryGetDist2Result(query, d_street_1, d_street_2, d_city, d_state, d_zip, d_name);
//        case QUERY_GET_ORDER_ID_PRE:
//            int no_o_id = Integer.parseInt(map1.get("no_o_id"));
//            return new QueryGetOrderIdResult(query, no_o_id);
        case QUERY_DELIVERY_GET_CUST_ID_PRE:
            int o_c_id = Integer.parseInt(map1.get("c_id"));
            return new QueryGetCustIdResult(query, o_c_id);
        case QUERY_GET_SUM_ORDER_AMOUNT_PRE:
            double ol_total = Double.parseDouble(map1.get("ol_amount"));
            return new QueryGetSumOrderAmountResult(query, ol_total);
        case QUERY_GET_ORDER_ID_PRE:
            return new QueryGetOrderIdResult(query, ids);
        case QUERY_STOCK_GET_ITEM_IDS_PRE:
            return new QueryStockGetItemIDs(query, map2);
        case QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD_PRE:
            return new QueryStockItemsEqualThreshold(query, ids);
        case QUERY_PAY_GET_CUST_BY_NAME_PRE:
            return new QueryGetCustomerByNameResult(query, ids);
        }
        return null;
    }


    // =========================//

    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt.
     * This will automatically call setObject for all the parameters you pass in
     * @param conn
     * @param stmt
     * @param parameters 
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt, Object...params) throws SQLException {
        PreparedStatement pStmt = this.getPreparedStatementReturnKeys(conn, stmt, null);
        for (int i = 0; i < params.length; i++) {
            pStmt.setObject(i+1, params[i]);
        } // FOR
        return (pStmt);
    }

    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt. 
     * @param conn
     * @param stmt
     * @param is 
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatementReturnKeys(Connection conn, SQLStmt stmt, int[] is) throws SQLException {
        assert(this.name_stmt_xref != null) : "The Procedure " + this + " has not been initialized yet!";
        PreparedStatement pStmt = this.prepardStatements.get(stmt);
        if (pStmt == null) {
            assert(this.stmt_name_xref.containsKey(stmt)) :
                "Unexpected SQLStmt handle in " + this.getClass().getSimpleName() + "\n" + this.name_stmt_xref;

            // HACK: If the target system is Postgres, wrap the PreparedStatement in a special
            //       one that fakes the getGeneratedKeys().
            if (is != null && this.dbType == DatabaseType.POSTGRES) {
                pStmt = new AutoIncrementPreparedStatement(this.dbType, conn.prepareStatement(stmt.getSQL()));
            }
            // Everyone else can use the regular getGeneratedKeys() method
            else if (is != null) {
                pStmt = conn.prepareStatement(stmt.getSQL(), is);
            }
            // They don't care about keys
            else {
                pStmt = conn.prepareStatement(stmt.getSQL());
            }
            this.prepardStatements.put(stmt, pStmt);
        }
        assert(pStmt != null) : "Unexpected null PreparedStatement for " + stmt;
        return (pStmt);
    }
    
    @Override
    public String getCollectionName(String queryOrDml) {
        if (TPCCConfig.useRangeQC) {
            String[] tokens = queryOrDml.split(",");
            
            switch (tokens[0]) {
            case QUERY_RANGE_GET_ITEMS_PRE:
            case DML_UPDATE_STOCK_PRE:
                int w_id = Integer.parseInt(tokens[1]);
                return String.valueOf(w_id);
            default:
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public Interval1D getBounds(String query) {
        String[] tokens = query.split(",");
        
        switch (tokens[0]) {
        case QUERY_RANGE_GET_ITEMS_PRE:
            int lb = Integer.parseInt(tokens[2]);
            int ub = Integer.parseInt(tokens[3]);
            return new Interval1D(lb, ub-1);
        default:
            return null;
        }
    }
    
    @Override
    public String constructFixPointQuery(String query, Object... params) {
        String[] tokens = query.split(",");
        
        if (params == null || params.length != 1) return null;
        
        switch (tokens[0]) {
        case QUERY_RANGE_GET_ITEMS_PRE:
            int w_id = Integer.parseInt(tokens[1]);
            return String.format(QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD, w_id, params[0]);
        default:
            return null;
        }
    }
    
    @Override
    public Map<Interval1D, List<Delta>> updatePoints(String dml) {
        String[] tokens = dml.split(",");
        
        Map<Interval1D, List<Delta>> m = new HashMap<>();
        
        switch (tokens[0]) {
        case DML_UPDATE_STOCK_PRE:      
            int new_p = Integer.parseInt(tokens[3]);
            int old_p = Integer.parseInt(tokens[7]);
            int i_id = Integer.parseInt(tokens[2]);
            
            Delta d = new Delta(Delta.TYPE_APPEND, i_id);
            List<Delta> ds =new ArrayList<>(); ds.add(d);
            m.put(new Interval1D(new_p,new_p), ds);
            
            d = new Delta(Delta.TYPE_RMW, i_id);
            ds =new ArrayList<>(); ds.add(d);
            m.put(new Interval1D(old_p,old_p), ds);
            
            return m;
        default:
            return null;
        }
    }
}