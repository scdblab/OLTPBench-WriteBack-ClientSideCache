package com.oltpbenchmark.benchmarks.tpcc;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.tpcc.procedures.QueryStockGetCountItems;
import com.oltpbenchmark.benchmarks.tpcc.procedures.QueryStockGetItemIDs;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustIdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetOrderIdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetSumOrderAmountResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryOrdStatGetNewestOrdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryOrderStatGetOrderLinesResult;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Session;

import edu.usc.dblab.intervaltree.Interval1D;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConfig.*;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConstants.*;

public class TPCCWriteBack extends WriteBack {
    private static final String INSERT = "I";
    private static final String SET = "S";
    private static final String INCR = "A";
    private static final String DELETE = "D";
    
    // new order
    public final SQLStmt stmtInsertNewOrderSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER + " (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ( ?, ?, ?)");
    public final SQLStmt stmtUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = ? AND D_ID = ?");
    public final SQLStmt stmtInsertOOrderSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER + " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" + " VALUES (?, ?, ?, ?, ?, ?, ?)");
    public final SQLStmt stmtUpdateStockSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_STOCK + " SET S_QUANTITY = ? , S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? " + " WHERE S_I_ID = ? AND S_W_ID = ?");
    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID," + "  OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?,?,?,?,?,?,?,?,?)");
    
    public final SQLStmt stmtInsertSessionIds = new SQLStmt("INSERT INTO COMMITED_SESSION VALUES (?)");
    public final SQLStmt stmtDeleteSessionIds = new SQLStmt("DELETE FROM COMMITED_SESSION WHERE sessid=?");

    //payment
    public SQLStmt payUpdateWhseSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET W_YTD = W_YTD + ?  WHERE W_ID = ? ");
    public SQLStmt payUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?");
    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ?, C_DATA = ? " + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payUpdateCustBalSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    public SQLStmt payInsertHistSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " + " VALUES (?,?,?,?,?,?,?,?)");

    public SQLStmt delivDeleteNewOrderSQL = new SQLStmt("DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER + "" + " WHERE NO_O_ID = ? AND NO_D_ID = ?" + " AND NO_W_ID = ?");
    public SQLStmt delivUpdateCarrierIdSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = ?" + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?");
    public SQLStmt delivUpdateDeliveryDateSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = ?" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?");
    public SQLStmt delivUpdateCustBalDelivCntSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = C_BALANCE + ?" + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1" + " WHERE C_W_ID = ?" + " AND C_D_ID = ?" + " AND C_ID = ?");
    
    private Connection conn;    
    
    private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();


    private static final int INT = 0;
    private static final int BIGDECIMAL = 1;
    private static final int STRING = 2;
    private static final int TIMESTAMP = 3;
    final Map<String, Integer> attributesTypes = new HashMap<>();
    private final int id;
    
    private Statement stmt;
    
    public TPCCWriteBack(Connection conn, int id) {
        this.id = id;
        this.conn = conn;
        
        try {
            this.stmt = conn.createStatement();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        initTypes();
    }
    
    @Override
    public boolean createSessionTable() {
//        if (com.usc.dblab.cafe.Config.storeCommitedSessions) {
            try {
                Statement statement = conn.createStatement();
                statement.execute("DROP TABLE IF EXISTS "
                        + "COMMITED_SESSION");
                statement.execute("CREATE TABLE IF NOT EXISTS "
                        + "COMMITED_SESSION(sessid VARCHAR(50) NOT NULL PRIMARY KEY)");
                conn.commit();
                statement.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }            
//        }
        
        return true;
    }
    
    @Override
    public boolean insertCommitedSessionRows(List<String> sessIds) {
        if (com.usc.dblab.cafe.Config.storeCommitedSessions) {
            try {
                PreparedStatement pStmt = getPreparedStatement(conn, stmtInsertSessionIds);
                for (String sessId: sessIds) {
                    pStmt.setString(1, sessId);
                    pStmt.addBatch();
                }
                pStmt.execute();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public boolean cleanupSessionTable(List<String> sessIds) {
        if (com.usc.dblab.cafe.Config.storeCommitedSessions) {
            try {
                PreparedStatement pStmt = getPreparedStatement(conn, stmtDeleteSessionIds);
                for (String sessId: sessIds) {
                    pStmt.setString(1, sessId);
                    pStmt.addBatch();
                }
                pStmt.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public List<String> checkExists(List<String> toExec) {
        if (com.usc.dblab.cafe.Config.storeCommitedSessions) {
            String query = "SELECT sessid FROM COMMITED_SESSION WHERE sessid IN (";
            for (String sessid: toExec) {
                query += "'"+sessid+"',";
            }
            query = query.substring(0, query.length()-1) + ")";
            
            try {
                ResultSet res = stmt.executeQuery(query);
                List<String> exists = new ArrayList<>();
                while (res.next()) {
                    exists.add(res.getString(1));
                }
                res.close();
                return exists;
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }
    
    private void initTypes() { 
        attributesTypes.put("W_ID", INT);
        attributesTypes.put("W_YTD", BIGDECIMAL);
        attributesTypes.put("W_TAX", BIGDECIMAL);
        attributesTypes.put("W_NAME", STRING);
        attributesTypes.put("W_STREET_1", STRING);
        attributesTypes.put("W_STREET_2", STRING);
        attributesTypes.put("W_CITY", STRING);
        attributesTypes.put("W_STATE", STRING);
        attributesTypes.put("W_ZIP", STRING);
 
        attributesTypes.put("D_W_ID", INT);        
        attributesTypes.put("D_ID", INT);        
        attributesTypes.put("D_NEXT_O_ID", INT);
        attributesTypes.put("D_YTD", BIGDECIMAL);
        attributesTypes.put("D_TAX", BIGDECIMAL);
        attributesTypes.put("D_NAME", STRING);
        attributesTypes.put("D_STREET_1", STRING);
        attributesTypes.put("D_STREET_2", STRING);
        attributesTypes.put("D_STATE", STRING);
        attributesTypes.put("D_ZIP", STRING);

        attributesTypes.put("O_ALL_LOCAL", INT);
        attributesTypes.put("O_C_ID", INT);
        attributesTypes.put("O_D_ID", INT);
        attributesTypes.put("O_ENTRY_D", TIMESTAMP);
        attributesTypes.put("O_ID", INT);
        attributesTypes.put("O_OL_CNT", INT);
        attributesTypes.put("O_W_ID", INT);
        attributesTypes.put("O_CARRIER_ID", INT);
        
        attributesTypes.put("NO_D_ID", INT);
        attributesTypes.put("NO_O_ID", INT);
        attributesTypes.put("NO_W_ID", INT);

        attributesTypes.put("OL_W_ID", INT);
        attributesTypes.put("OL_D_ID", INT);
        attributesTypes.put("OL_O_ID", INT);
        attributesTypes.put("OL_NUMBER", INT);
        attributesTypes.put("OL_I_ID", INT);
        attributesTypes.put("OL_DELIVERY_D", TIMESTAMP);
        attributesTypes.put("OL_AMOUNT", BIGDECIMAL);
        attributesTypes.put("OL_SUPPLY_W_ID", INT);
        attributesTypes.put("OL_QUANTITY", INT);
        attributesTypes.put("S_ORDER_CNT", INT);
        attributesTypes.put("OL_DIST_INFO", STRING);
        
        attributesTypes.put("C_BALANCE", BIGDECIMAL);
        attributesTypes.put("C_DATA", STRING);
        attributesTypes.put("C_PAYMENT_CNT", INT);
        attributesTypes.put("C_YTD_PAYMENT", BIGDECIMAL);
        attributesTypes.put("C_D_ID", INT);
        attributesTypes.put("C_W_ID", INT);
        attributesTypes.put("C_ID", INT);
        
        attributesTypes.put("S_ORDER_CNT", INT);
        attributesTypes.put("S_QUANTITY", INT);
        attributesTypes.put("S_YTD", INT);
        attributesTypes.put("S_I_ID", INT);
        attributesTypes.put("S_W_ID", INT);
        
        attributesTypes.put("H_C_ID", INT);
        attributesTypes.put("H_C_D_ID", INT);
        attributesTypes.put("H_C_W_ID", INT);
        attributesTypes.put("H_D_ID", INT);
        attributesTypes.put("H_W_ID", INT);
        attributesTypes.put("H_DATE", TIMESTAMP);
        attributesTypes.put("H_AMOUNT", BIGDECIMAL);
        attributesTypes.put("H_DATA", STRING);
        
        
    }

    @Override
    public Set<String> getMapping(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LinkedHashMap<String, Change> bufferChanges(String dml, Set<String> buffKeys) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean applyBufferedWrite(String buffKey, Object buffValue) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isIdempotent(Object buffValue) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object convertToIdempotent(Object buffValue) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> rationalizeRead(String query) {
        String[] tokens = query.split(",");
        Set<String> set = new HashSet<>();
        switch (tokens[0]) {
        // NewOrder
        case QUERY_DISTRICT_NEXT_ORDER_PRE:
            set.add(String.format(DATA_ITEM_DISTRICT, tokens[1], tokens[2]));
            break;
        case QUERY_GET_STOCK_PRE:
            set.add(String.format(DATA_ITEM_STOCK, tokens[1], tokens[2]));
            break;
            
        // Payment
        case QUERY_PAY_GET_CUST_PRE:
            set.add(String.format(DATA_ITEM_CUSTOMER, tokens[1], tokens[2], tokens[3]));
            break;
        case QUERY_GET_CUST_C_DATA_PRE:
            set.add(String.format(DATA_ITEM_CUSTOMER, tokens[1], tokens[2], tokens[3]));
            break;            

        // Delivery
        case QUERY_GET_ORDER_ID_PRE:
            set.add(String.format(DATA_ITEM_NEW_ORDER, tokens[1], tokens[2]));
            break;
        case QUERY_DELIVERY_GET_CUST_ID_PRE:
            set.add(String.format(DATA_ITEM_ORDER, tokens[1], tokens[2]));
            break;            
        case QUERY_GET_SUM_ORDER_AMOUNT_PRE:
            set.add(String.format(DATA_ITEM_ORDER_LINE, tokens[1], tokens[2], tokens[3]));
            break;
            
        // OrderStatus
        case QUERY_ORDER_STAT_GET_NEWEST_ORDER_PRE:
            set.add(String.format(DATA_ITEM_ORDER, tokens[1], tokens[2]));
            break;
        case QUERY_ORDER_STAT_GET_ORDER_LINES_PRE:
            set.add(String.format(DATA_ITEM_ORDER_LINE, tokens[1], tokens[2], tokens[3]));
            break;
            
        // StockLevel
        // case QUERY_DISTRICT_NEXT_ORDER_PRE:  see above 
        case QUERY_STOCK_GET_ITEM_IDS_PRE:
            int x = Integer.parseInt(tokens[3]);
            for (int i = x-20; i < x; ++i) {
                set.add(String.format(DATA_ITEM_ORDER_LINE, tokens[1], tokens[2], i));
            }
            break;
        case QUERY_STOCK_COUNT_ITEMS_IDS_PRE:
            for (int i = 3; i < tokens.length; ++i) {
                set.add(String.format(DATA_ITEM_STOCK, tokens[1], tokens[i]));
            }
            break;            
        }
        return set;
    }

    @Override
    public LinkedHashMap<String, Change> rationalizeWrite(String dml) {
        String[] tokens = dml.split(",");
        LinkedHashMap<String, Change> map = new LinkedHashMap<>();
        String it = null;
        Change c = null;
        
        switch (tokens[0]) {
        // NewOrder        
        case DML_UPDATE_DIST_PRE:
            c = new Change(Change.TYPE_RMW, INCR+",D_NEXT_O_ID,1");
            it = String.format(DATA_ITEM_DISTRICT, tokens[1], tokens[2]);
            break;
        case DML_INSERT_NEW_ORDER_PRE:	        
            c = new Change(Change.TYPE_RMW, String.format("%s,NO_O_ID,%s", INSERT, tokens[3]));
            it = String.format(DATA_ITEM_NEW_ORDER, tokens[1], tokens[2]);
            break;
        case DML_INSERT_OORDER_PRE:
            it = String.format(DATA_ITEM_ORDER, tokens[1], tokens[2]);
            c = new Change(Change.TYPE_RMW, String.format(INSERT+",O_C_ID,%s,O_ID,%s,O_ENTRY_D,%s,O_OL_CNT,%s,O_ALL_LOCAL,%s", 
                    tokens[3], tokens[4], tokens[5], tokens[6], tokens[7]));
            break;
        case DML_UPDATE_STOCK_PRE:
            c = new Change(Change.TYPE_RMW, String.format("%s,S_QUANTITY,%s,%s;%s,S_YTD,%s;%s,S_ORDER_CNT,1", 
                    SET, tokens[3], tokens[7], INCR, tokens[4], INCR));
            it = String.format(DATA_ITEM_STOCK, tokens[1], tokens[2]);
            break;
        case DML_INSERT_ORDER_LINE_PRE:
            c = new Change(Change.TYPE_RMW,
                    String.format(INSERT+",OL_NUMBER,%s,OL_I_ID,%s,OL_SUPPLY_W_ID,%s,OL_QUANTITY,%s,OL_AMOUNT,%s,OL_DIST_INFO,%s", 
                            tokens[4], tokens[5], tokens[6], tokens[7], tokens[8], tokens[9]));
            it = String.format(DATA_ITEM_ORDER_LINE, tokens[1], tokens[2], tokens[3]);
            break;
            
        // payment
        case DML_UPDATE_WHSE_PRE:
            c = new Change(Change.TYPE_RMW,
                    String.format("%s,W_YTD,%s", INCR, tokens[2]));
            it = String.format(DATA_ITEM_WAREHOUSE, tokens[1]);
            break;
        case DML_PAY_UPDATE_DIST_PRE:
            c = new Change(Change.TYPE_RMW, 
                    String.format("%s,D_YTD,%s", INCR, tokens[3]));
            it = String.format(DATA_ITEM_DISTRICT, tokens[1], tokens[2]);
            break;
        case DML_UPDATE_CUST_BAL_PRE:
            c = new Change(Change.TYPE_RMW,
                    String.format("%s,C_BALANCE,%s;%s,C_YTD_PAYMENT,%s;%s,C_PAYMENT_CNT,%s",
                            SET, tokens[4], SET, tokens[5], SET, tokens[6]));
            it = String.format(DATA_ITEM_CUSTOMER, tokens[1], tokens[2], tokens[3]);
            break;
        case DML_UPDATE_CUST_BAL_C_DATA_PRE:
            c = new Change(Change.TYPE_RMW,
                    String.format("%s,C_BALANCE,%s;%s,C_YTD_PAYMENT,%s;%s,C_PAYMENT_CNT,%s;%s,C_DATA,%s",
                            SET, tokens[4], SET, tokens[5], SET, tokens[6], SET, tokens[7]));
            it = String.format(DATA_ITEM_CUSTOMER, tokens[1], tokens[2], tokens[3]);
            break;
        case DML_INSERT_HISTORY_PRE:
            c = new Change(Change.TYPE_RMW,
                    String.format(INSERT+",H_C_D_ID,%s,H_C_W_ID,%s,H_C_ID,%s,H_D_ID,%s,H_W_ID,%s,H_DATE,%s,H_AMOUNT,%s,H_DATA,%s", tokens[2], tokens[1],  
                            tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], tokens[8]));
            it = String.format(DATA_ITEM_HISTORY, tokens[1], tokens[2], tokens[3]);
            break;
            
        // delivery
        case DML_DELETE_NEW_ORDER_PRE:
            c = new Change(Change.TYPE_RMW,
                    String.format(DELETE+",NO_O_ID,%s", tokens[3]));
            it = String.format(DATA_ITEM_NEW_ORDER, tokens[1], tokens[2]);
            break;
        case DML_UPDATE_CARRIER_ID_PRE:
            c = new Change(Change.TYPE_RMW,
                    String.format("%s,O_CARRIER_ID,%s;O_ID,%s", SET, tokens[4], tokens[3]));
            it = String.format(DATA_ITEM_ORDER, tokens[1], tokens[2]);
            break;
        case DML_UPDATE_DELIVERY_DATE_PRE:	        
            c = new Change(Change.TYPE_RMW,
                    String.format("%s,OL_DELIVERY_D,%s", SET, tokens[4]));
            it = String.format(DATA_ITEM_ORDER_LINE, tokens[1], tokens[2], tokens[3]);
            break;
        case DML_UPDATE_CUST_BAL_DELIVERY_CNT_PRE:
            c = new Change(Change.TYPE_RMW,
                    String.format("%s,C_BALANCE,%s;C_DELIVERY_CNT,1s", INCR, tokens[4]));
            it = String.format(DATA_ITEM_CUSTOMER, tokens[1], tokens[2], tokens[3]);
            break;
        }

        if (c != null) map.put(it, c);
        return map;
    }

    @Override
    public byte[] serialize(Change change) {
        if (change.getType() != Change.TYPE_RMW)
            throw new NotImplementedException("Should not have change of type different than RMW");
        int len = 0;
//        String sid = change.getSid();
//        len += 4+sid.length();

//        int sequenceId = change.getSequenceId();
//        len += 4;

        String val = (String) change.getValue();
        len += 4+val.length();

        ByteBuffer buff = ByteBuffer.allocate(len);
//        buff.putInt(sid.length());
//        buff.put(sid.getBytes());
//        buff.putInt(sequenceId);
        buff.putInt(val.length());
        buff.put(val.getBytes());
        return buff.array();
    }

    @Override
    public Change deserialize(byte[] bytes) {
        ByteBuffer buff = ByteBuffer.wrap(bytes);
//        int len = buff.getInt();
//        byte[] bs = new byte[len];
//        buff.get(bs);
//        String sid = new String(bs);

//        int seqId = buff.getInt();

        int len = buff.getInt();
        byte[] bs = new byte[len];
        buff.get(bs);
        String val = new String(bs);
        Change change = new Change(Change.TYPE_RMW, val);
//        change.setSid(sid);
//        change.setSequenceId(seqId);
        return change;
    }    
    
    public boolean applySessions(List<Session> sessList, Connection conn, Statement stmt,
            PrintWriter sessionWriter, Stats stats) throws SQLException {
        StringBuilder sb = new StringBuilder();
        String table = null;
        
        List<DML> districtDmls = new ArrayList<>();
        List<DML> oorderDmls = new ArrayList<>();
        List<DML> orderLineDmls = new ArrayList<>();
        List<DML> customerDmls = new ArrayList<>();
        List<DML> newOrderDmls = new ArrayList<>();
        List<DML> stockDmls = new ArrayList<>();
        List<DML> warehouseDmls = new ArrayList<>();
        List<DML> historyDmls = new ArrayList<>();
        
        
        List<DML> dmls = null;
        List<DML> localDmls = new ArrayList<>();
        
        List<String> lines = new ArrayList<>();
        
        for (Session sess: sessList) {
            localDmls.clear();
            
//            System.out.println("Apply session "+sess.getSid());
            if (sessionWriter != null) {
                sessionWriter.println(sess.getSid());
            }
            lines.add(sess.getSid());
            
            List<String> its = sess.getIdentifiers();
            
            for (int i = 0; i < its.size(); ++i) {
                TreeMap<String, String> whereClause=new TreeMap<>();
                TreeMap<String, String> updateClause = new TreeMap<>();
                sb.setLength(0);
                
                String it = its.get(i);
                Change c = sess.getChange(i);
                String[] tokens = it.split(",");
                switch (tokens[0]) {
                case DATA_ITEM_DISTRICT_PRE:
                    table = "DISTRICT";
                    whereClause.put("D_W_ID", tokens[1]);
                    whereClause.put("D_ID", tokens[2]);
                    dmls = districtDmls;
                    break;
                case DATA_ITEM_ORDER_PRE:
                    table = "OORDER";
                    whereClause.put("O_W_ID", tokens[1]);
                    whereClause.put("O_D_ID", tokens[2]);
                    dmls = oorderDmls;
                    break;
                case DATA_ITEM_ORDER_LINE_PRE:
                    table = "ORDER_LINE";
                    whereClause.put("OL_W_ID", tokens[1]);
                    whereClause.put("OL_D_ID", tokens[2]);
                    whereClause.put("OL_O_ID", tokens[3]);
                    dmls = orderLineDmls;
                    break;
                case DATA_ITEM_NEW_ORDER_PRE:
                    table = "NEW_ORDER";
                    whereClause.put("NO_W_ID", tokens[1]);
                    whereClause.put("NO_D_ID", tokens[2]);
                    dmls = newOrderDmls;
                    break;
                case DATA_ITEM_STOCK_PRE:
                    table = "STOCK";
                    whereClause.put("S_W_ID", tokens[1]);
                    whereClause.put("S_I_ID", tokens[2]);
                    dmls = stockDmls;
                    break;
                case DATA_ITEM_WAREHOUSE_PRE:
                    table = "WAREHOUSE";
                    whereClause.put("W_ID", tokens[1]);
                    dmls = warehouseDmls;
                    break;
                case DATA_ITEM_CUSTOMER_PRE:
                    table = "CUSTOMER";
                    whereClause.put("C_W_ID", tokens[1]);
                    whereClause.put("C_D_ID", tokens[2]);
                    whereClause.put("C_ID", tokens[3]);
                    dmls = customerDmls;
                    break;
                case DATA_ITEM_HISTORY_PRE:
                    table = "HISTORY";
                    dmls = historyDmls;
                    break;
                }
                
                if (table == null) {
                    System.out.println("Unknown table.");
                    System.exit(-1);
                }                        
                                        
                String val = (String)c.getValue();
                tokens = val.split(";");
                boolean insert = false;
                boolean delete = false;
                for (String token: tokens) {
                    String[] params = token.split(",");
                    switch (params[0]) {
                    case INCR:
                        updateClause.put(params[1], params[1]+"+"+params[2]);
                        break;
                    case SET:
                        updateClause.put(params[1], params[2]);
                        break;
                    case INSERT:
                        insert = true;
                        for (int j = 1; j < params.length; j+=2) {
                            updateClause.put(params[j],  params[j+1]);
                        }
                        break;
                    case DELETE:
                        delete = true;
                        for (int j = 1; j < params.length; j+=2) {
                            updateClause.put(params[j], params[j+1]);
                        }
                        break;
                    }
                }
                
                DML dml = null;
                if (insert) {
                    updateClause.putAll(whereClause);
                    whereClause.clear();                            
                    dml = new DML(table, DML.INSERT, updateClause, whereClause);
                } else if (delete) {
                    whereClause.putAll(updateClause);
                    updateClause.clear();
                    dml = new DML(table, DML.DELETE, updateClause, whereClause);                            
                } else {
                    dml = new DML(table, DML.UPDATE, updateClause, whereClause);
                }                  
                
                dmls.add(dml);
                localDmls.add(dml);
            }
            
            for (DML dml: localDmls) {
                if (sessionWriter != null) {
                    sessionWriter.println(dml.toSQL());
                }
                
                lines.add(dml.toSQL());
            }
        }
        
        for (int i = 0; i < lines.size(); ++i) {
            String line = lines.get(i);
            
            String token = "UPDATE ORDER_LINE SET OL_DELIVERY_D=";
            if (line.contains(token)) {
                long ts = Long.parseLong(line.substring(token.length()).split(" ")[0]);
                Date date = new Date(ts);
                String s = date.toString();
                line = line.replace(String.valueOf(ts), "'"+s+"'");
                lines.set(i, line);
            }
            
            if (line.contains("INSERT INTO HISTORY")) {
                String s = line.split(",")[11];
                line = line.replace(s, "'"+s+"'");
                long ts = Long.parseLong(line.split(",")[12]);
                Date date = new Date(ts);
                s = date.toString();
                line = line.replace(String.valueOf(ts), "'"+s+"'");
                lines.set(i, line);
            }
            
            if (line.contains("INSERT INTO OORDER")) {
                long ts = Long.parseLong(line.split(",")[9]);
                Date date = new Date(ts);
                String s = date.toString();
                line = line.replace(String.valueOf(ts), "'"+s+"'");
                lines.set(i, line);
            }
            
            if (line.contains("INSERT INTO ORDER_LINE")) {
                String s= line.split(",")[9];
                line = line.replace(s, "'"+s+"'");
                lines.set(i, line);
            }
            
            if (line.contains("UPDATE CUSTOMER SET C_BALANCE") && line.contains("C_DATA")) {
                String s = line.split(",")[1].split("=")[1];
                line = line.replace(s, "'"+s+"'");
                lines.set(i, line);
            }
        }
        
        TPCCOptimization.exec(Config.BATCH, true, lines, stats, conn, stmt, this);

        return true;
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
    public QueryResult merge(String query, QueryResult result,
            LinkedHashMap<String, List<Change>> buffVals) {
        if (buffVals == null || buffVals.size() == 0) return result;
        
        String tokens[] = query.split(",");
        
        // since each query impacts only one data item, get the change list.
        List<Change> changes = buffVals.values().iterator().next();
        
        switch (tokens[0]) {
        case QUERY_GET_ORDER_ID_PRE:
            List<Integer> list;
            if (result != null) list = ((QueryGetOrderIdResult) result).getNewOrderIds();
            else list = new ArrayList<>();
            for (Change c: changes) {
                String val = (String)c.getValue();
                String[] fs = val.split(",");
                switch (fs[0]) {
                case INSERT:
                    list.add(Integer.parseInt(fs[2]));
                    break;
                case DELETE:
                    list.remove(Integer.parseInt(fs[2]));
                    break;
                }
            }       
            return new QueryGetOrderIdResult(query, list);
        case QUERY_DELIVERY_GET_CUST_ID_PRE:
//            if (result != null) return result;
            for (Change c: changes) {
                String val = (String)c.getValue();                
                String[] fs = val.split(",");
                switch (fs[0]) {
                case INSERT:
                    for (int i = 1; i < fs.length; i+=2) {
                        if (fs[i].equals("O_C_ID")) {
                            int c_id = Integer.parseInt(fs[i+1]);
                            return new QueryGetCustIdResult(query, c_id);
                        }
                    }
                    break;
                }
            }            
            return result;
        case QUERY_GET_SUM_ORDER_AMOUNT_PRE: 
            int ol_o_id = Integer.parseInt(tokens[3]);
            double amount = 0.0;            
            if (result != null) {
                QueryGetSumOrderAmountResult res = (QueryGetSumOrderAmountResult) result;
                amount = res.getTotal();
            }
            
            for (Change c: changes) {
                String val = (String)c.getValue();                
                String[] fs = val.split(",");
                double x = 0.0;
                boolean found = false;
                
                switch (fs[0]) {
                case INSERT:
                    for (int i = 1; i < fs.length; i+=2) {
                        if (fs[i].equals("OL_O_ID")) {
                            int id = Integer.parseInt(fs[i+1]);
                            if (id != ol_o_id) break;
                            found = true;
                        }
                        
                        if (fs[i].equals("OL_AMOUNT")) {
                            x += Double.parseDouble(fs[i+1]);
                        }
                    }
                    break;
                }
                if (found) amount += x;
            }
            return new QueryGetSumOrderAmountResult(query, amount);
        case QUERY_STOCK_GET_ITEM_IDS_PRE:
            int o_id = Integer.parseInt(tokens[3]);
            Set<Integer> itemIds = new HashSet<>();
            for (Change c: changes) {
                String s = (String)c.getValue();
                tokens = s.split(",");
                for (int i = 0; i < tokens.length; ++i) {
                    if (tokens[i].equals("OL_I_ID")) {
                        itemIds.add(Integer.parseInt(tokens[i+1]));
                    }
                }
            }
            QueryStockGetItemIDs res2 = (QueryStockGetItemIDs) result;
            Map<Integer, Set<Integer>> map = res2.getItemIDs();
            Set<Integer> set = map.get(o_id);
            if (set == null) map.put(o_id, itemIds);
            else set.addAll(itemIds);
            break;
        case QUERY_STOCK_COUNT_ITEMS_IDS_PRE:
            QueryStockGetCountItems res3 = (QueryStockGetCountItems) result;
            int stockCount = res3.getStockCount();
            res3 = new QueryStockGetCountItems(res3.getQuery(), stockCount);
            break;
        case QUERY_ORDER_STAT_GET_NEWEST_ORDER_PRE:
            QueryOrdStatGetNewestOrdResult res4 = (QueryOrdStatGetNewestOrdResult) result;
            for (Change c: changes) {
                String val = (String)c.getValue();
                String[] fs = val.split(";");
                for (String f: fs) {
                    tokens = f.split(",");
                    switch (tokens[0]) {
                    case SET:
                        if (tokens[1].equals("O_CARRIER_ID")) {
                            res4.setCarrierId(Integer.parseInt(tokens[2]));
                        } else if (tokens[1].equals("O_ID")) {
                            res4.setOId(Integer.parseInt(tokens[2]));
                        } else if (tokens[1].equals("O_DELIVERY_D")) {
                            res4.setDeliveryDate(Long.parseLong(tokens[2]));
                        }
                        break;
                    }                    
                }
            }     
            break;
        case QUERY_ORDER_STAT_GET_ORDER_LINES_PRE:
            QueryOrderStatGetOrderLinesResult res5 = (QueryOrderStatGetOrderLinesResult) result;
            int count = 0;
            for (Change c: changes) {
                String val = (String)c.getValue();
                String[] fs = val.split(";");
                for (String f: fs) {
                    tokens = f.split(",");
                    switch (tokens[0]) {
                    case SET:
                        if (tokens[1].equals("OL_DELIVERY_D")) {
                            res5.setDeliveryDate(Long.parseLong(tokens[2]));
                        }
                        break;
                    case INSERT:
                        for (int i = 1; i < fs.length; i+=2) {
                            count++;
                        }
                        break;
                    }                    
                }
            }
            res5.setOrderLinesCount(count);
            break;
        }
        
        return result;
    }

    @Override
    public Map<String, List<Change>> deserializeSessionChanges(byte[] bytes) {
        ByteBuffer buff = ByteBuffer.wrap(bytes);

        int offset = 0;
        LinkedHashMap<String, List<Change>> changesMap = new LinkedHashMap<>();
        while (offset < bytes.length) {
            int sz = buff.getInt();
            byte[] b = new byte[sz];
            buff.get(b);
            String it = new String(b);
            offset += 4+sz;

            sz = buff.getInt();
            offset += 4;

            List<Change> changes = new ArrayList<>();
            for (int i = 0; i < sz; ++i) {
                int len = buff.getInt();
                offset += 4;

                b = new byte[len];
                buff.get(b);
                offset += len;
                Change c = deserialize(b);
                changes.add(c);
            }

            changesMap.put(it, changes);
        }

        return changesMap;
    }

    @Override
    public int getTeleWPartition(String sessId) {
        return id;
    }

    @Override
    public byte[] serializeSessionChanges(Map<String, List<Change>> changesMap) {
        int totalSize = 0;

        LinkedHashMap<String, byte[][]> bytesMap = new LinkedHashMap<>();
        for (String it: changesMap.keySet()) {
            totalSize += 4+it.length();

            List<Change> changes = changesMap.get(it);
            totalSize += 4; // storing the size of the list

            byte[][] bytesList = new byte[changes.size()][];
            for (int i = 0; i < changes.size(); ++i) {
                Change c = changes.get(i);
                byte[] bytes = serialize(c);
                bytesList[i] = bytes;
                
                totalSize += 4;
                totalSize += bytes.length;
            }

            bytesMap.put(it, bytesList);
        }

        ByteBuffer buff = ByteBuffer.allocate(totalSize);
        for (String it: changesMap.keySet()) {
            buff.putInt(it.length());
            buff.put(it.getBytes());

            byte[][] bytesList = bytesMap.get(it);
            buff.putInt(bytesList.length);
            for (int i = 0; i < bytesList.length; ++i) {
                buff.putInt(bytesList[i].length);
                buff.put(bytesList[i]);
            }
        }

        return buff.array();
    }
    
    @Override
    public Map<Interval1D, List<Change>> bufferPoints(String dml) {
        String[] tokens = dml.split(",");
        
        Map<Interval1D, List<Change>> m = new HashMap<>();
        switch (tokens[0]) {
        case DML_UPDATE_STOCK_PRE:
            int p1 = Integer.parseInt(tokens[3]);
            int p2 = Integer.parseInt(tokens[7]);
            int i_id = Integer.parseInt(tokens[2]);
            Change d = new Change(Change.TYPE_APPEND, INSERT+","+i_id);
            List<Change> cs = new ArrayList<>(); cs.add(d);
            m.put(new Interval1D(p1,p1), cs);
            
            d = new Change(Change.TYPE_APPEND, DELETE+","+i_id);
            cs = new ArrayList<>(); cs.add(d);
            m.put(new Interval1D(p2,p2), cs);
            return m;
        default:
            return null;
        }
    }
    
    @Override
    public Map<Interval1D, String> getImpactedRanges(Session sess) {
        List<Change> changes = sess.getChanges();
        Map<Interval1D, String> res = new HashMap<>();
        for (Change c: changes) {
            String str = (String)c.getValue();
            if (str.contains("S_QUANTITY")) {
                String[] fs = str.split(";");
                for (String f: fs) {
                    if (f.contains("S_QUANTITY")) {
                        fs = f.split(",");
                        int p1 = Integer.parseInt(fs[2]);
                        res.put(new Interval1D(p1, p1), sess.getSid());
                        int p2 = Integer.parseInt(fs[3]);
                        res.put(new Interval1D(p2, p2), sess.getSid());
                        return res;
                    }
                }
            }
        }
        
        return res;
    }
}
