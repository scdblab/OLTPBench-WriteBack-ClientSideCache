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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.meetup.memcached.COException;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetDistResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryStockItemsEqualThreshold;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.QueryResult;

public class StockLevel extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevel.class);

    public SQLStmt stockGetDistOrderIdSQL = new SQLStmt("SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?");

    public SQLStmt stockGetCountStockSQL = new SQLStmt("SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT"
            + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK
            + " WHERE OL_W_ID = ?"
            + " AND OL_D_ID = ?"
            + " AND OL_O_ID < ?"
            + " AND OL_O_ID >= ? - 20"
            + " AND S_W_ID = ?"
            + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?");
    
    public static final int SQL_INGROUP=11;
    public SQLStmt stockGetItemIdsSQL = new SQLStmt("SELECT DISTINCT(OL_I_ID) FROM " + TPCCConstants.TABLENAME_ORDERLINE + " WHERE OL_O_ID < ? AND OL_O_ID >= ? - 20" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?");
    public SQLStmt stockCountItemsIdsSQL = new SQLStmt("SELECT COUNT(DISTINCT(S_I_ID)) FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_W_ID = ?" + " AND S_I_ID IN (?,?,?,?,?,?,?,?,?,?)" + " AND S_QUANTITY < ?");

    // Stock Level Txn
    private PreparedStatement stockGetDistOrderId = null;
    private PreparedStatement stockGetCountStock = null;

    public ResultSet run(Connection conn, Random gen,
            int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID, Map<String, Object> tres) throws SQLException {
        stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);
        stockGetCountStock= this.getPreparedStatement(conn, stockGetCountStockSQL);

        int threshold = TPCCUtil.randomNumber(10, 20, gen);
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);

        if (Config.DEBUG) {
            out.println(String.format("Stock Level d_id=%d, threshold=%d", districtID, threshold));
        }
        
        stockLevelTransaction(terminalWarehouseID, districtID, threshold,conn, tres);

        return null;
    }

    public void stockLevelTransaction(int w_id, int d_id, int threshold, 
            Connection conn, Map<String, Object> tres)
            throws SQLException {
        int o_id = 0;
        int stock_count = 0;

        stockGetDistOrderId.setInt(1, w_id);
        stockGetDistOrderId.setInt(2, d_id);
        ResultSet rs = stockGetDistOrderId.executeQuery();

        if (!rs.next())
            throw new RuntimeException("D_W_ID="+ w_id +" D_ID="+ d_id+" not found!");
        o_id = rs.getInt("D_NEXT_O_ID");
        rs.close();
        rs = null;
        
        PreparedStatement stockGetItemIds = this.getPreparedStatement(conn, stockGetItemIdsSQL);
        stockGetItemIds.setInt(1, o_id);
        stockGetItemIds.setInt(2, o_id);
        stockGetItemIds.setInt(3, d_id);
        stockGetItemIds.setInt(4, w_id);
        rs = stockGetItemIds.executeQuery();
        List<Integer> itemIds = new ArrayList<>();
        while (rs.next()) {
            int id = rs.getInt(1);
            itemIds.add(id);
        }
        rs.close();
        rs = null;
        
        stockGetCountStock.setInt(1, w_id);
        stockGetCountStock.setInt(2, d_id);
        stockGetCountStock.setInt(3, o_id);
        stockGetCountStock.setInt(4, o_id);
        stockGetCountStock.setInt(5, w_id);
        stockGetCountStock.setInt(6, threshold);
        rs = stockGetCountStock.executeQuery();
        if (rs.next()) {
            stock_count = rs.getInt(1);
        }
        rs.close();
        rs = null;
  
//        PreparedStatement stockCountItemsIds = this.getPreparedStatement(conn, stockCountItemsIdsSQL);
//        int cnt = 1;
//        int stockCount = 0;
//        for (int itemId: itemIds) {
//            if (cnt == 1) {
//                stockCountItemsIds.setInt(1, w_id);
//            }
//            
//            if (cnt < SQL_INGROUP) {
//                stockCountItemsIds.setInt(++cnt, itemId);
//                if (cnt == SQL_INGROUP) {
//                    stockCountItemsIds.setInt(1+SQL_INGROUP, threshold);                        
//                    rs = stockCountItemsIds.executeQuery();
//                    rs.next();
//                    stockCount += rs.getBigDecimal(1).intValue();
//                    rs.close();
//                    rs = null;
//                    
//                    cnt = 1;
//                }
//            }
//        }
//        if (cnt > 1) {
//            int x = itemIds.get(0);
//            
//            // fill the remaining with same value
//            while (cnt < SQL_INGROUP) {
//                stockCountItemsIds.setInt(++cnt, x);
//                
//            }
//            stockCountItemsIds.setInt(1+SQL_INGROUP, threshold);
//            rs = stockCountItemsIds.executeQuery();
//            rs.next();
//            stockCount += rs.getBigDecimal(1).intValue();
//            rs.close();
//            rs = null;
//        }

        conn.commit();


        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage
        .append("\n+-------------------------- STOCK-LEVEL --------------------------+");
        terminalMessage.append("\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n\n Stock Level Threshold: ");
        terminalMessage.append(threshold);
        terminalMessage.append("\n Low Stock Count:       ");
        terminalMessage.append(stock_count);
        terminalMessage
        .append("\n+-----------------------------------------------------------------+\n\n");
        
        if (Config.ENABLE_LOGGING) {
            tres.put("w_id", w_id);
            tres.put("d_id", d_id);
            tres.put("T", threshold);
            tres.put("d_next_o_id", o_id);
            
            Collections.sort(itemIds);
            tres.put("item_ids", Arrays.toString(itemIds.toArray(new Integer[0])));
            
            tres.put("stock_count", stock_count);
        }
        
        if(LOG.isTraceEnabled())LOG.trace(terminalMessage.toString());
    }

    public void stockLevelTransaction(int terminalWarehouseID, int districtID, int threshold, 
            NgCache cafe, Connection conn, Map<String, Object> tres) {
        int retry = 0;
        int o_id = 0;
        Set<Integer>  itemIds = new TreeSet<>();
        int stock_count = 0;
        while (true) {
            stock_count = 0;
            itemIds.clear();
            
            try {
                cafe.startSession("StockLevel");

                String queryDistOrder = String.format(TPCCConfig.QUERY_DISTRICT_NEXT_ORDER, terminalWarehouseID, districtID);
                QueryGetDistResult res = (QueryGetDistResult) cafe.readStatement(queryDistOrder);
                if (res == null)
                    throw new RuntimeException("D_W_ID="+ terminalWarehouseID +" D_ID="+ districtID +" not found!");
                o_id = res.getNextOrderId();

                String queryStockGetItemIDs = String.format(TPCCConfig.QUERY_STOCK_GET_ITEM_IDS, terminalWarehouseID, districtID, o_id);
                QueryStockGetItemIDs rs = (QueryStockGetItemIDs) cafe.readStatement(queryStockGetItemIDs);
                if (rs == null) {
                    throw new RuntimeException("OL_W_ID="+terminalWarehouseID +" OL_D_ID="+districtID+" OL_O_ID="+o_id+" not found!");
                }
                Map<Integer, Set<Integer>> itemIdsMap = rs.getItemIDs();
                for (Integer ol_o_id: itemIdsMap.keySet()) {
                    itemIds.addAll(itemIdsMap.get(ol_o_id));
                }
                cafe.getStats().incrBy("stock_level_item_ids", itemIds.size());
                
                String query = Arrays.toString(itemIds.toArray(new Integer[0])).replaceAll(" |\\[|\\]", "");
                query = String.format(TPCCConfig.QUERY_STOCK_COUNT_ITEMS_IDS, terminalWarehouseID, threshold, query);
                QueryStockGetCountItems rs2 = (QueryStockGetCountItems) cafe.readStatement(query);
                if (rs2 == null) {
                    throw new RuntimeException("OL_W_ID="+terminalWarehouseID +" S_I_IDS not found!");
                }
                stock_count = rs2.getStockCount();

                conn.commit();
                cafe.commitSession();

                StringBuilder terminalMessage = new StringBuilder();
                terminalMessage
                .append("\n+-------------------------- STOCK-LEVEL --------------------------+");
                terminalMessage.append("\n Warehouse: ");
                terminalMessage.append(terminalWarehouseID);
                terminalMessage.append("\n District:  ");
                terminalMessage.append(districtID);
                terminalMessage.append("\n\n Stock Level Threshold: ");
                terminalMessage.append(threshold);
                terminalMessage.append("\n Low Stock Count:       ");
                terminalMessage.append(stock_count);
                terminalMessage
                .append("\n+-----------------------------------------------------------------+\n\n");
                if(LOG.isTraceEnabled())LOG.trace(terminalMessage.toString());

                break;
            } catch (Exception e) {
//                e.printStackTrace(System.out);
                
                if (e instanceof COException) {
                    cafe.getStats().incr(((COException) e).getKey());
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
        
        if (Config.ENABLE_LOGGING) {
            tres.put("w_id", terminalWarehouseID);
            tres.put("d_id", districtID);
            tres.put("T", threshold);
            tres.put("d_next_o_id", o_id);
            tres.put("item_ids", Arrays.toString(itemIds.toArray(new Integer[0])));
            tres.put("stock_count", stock_count);
        }
    }

    public void stockLevelTransaction2(int terminalWarehouseID, int districtID, int threshold, 
            NgCache cafe, Connection conn, Map<String, Object> tres) {
        int retry = 0;
        int o_id = 0;
        int stock_count = 0;
        Set<Integer>  itemIds = new TreeSet<>();
        while (true) {
            stock_count = 0;
            itemIds.clear();
            
            try {
                cafe.startSession("StockLevel", String.valueOf(terminalWarehouseID));

                String queryDistOrder = String.format(TPCCConfig.QUERY_DISTRICT_NEXT_ORDER, terminalWarehouseID, districtID);
                QueryGetDistResult res = (QueryGetDistResult) cafe.readStatement(queryDistOrder);
                if (res == null)
                    throw new RuntimeException("D_W_ID="+ terminalWarehouseID +" D_ID="+ districtID +" not found!");
                o_id = res.getNextOrderId();

                String queryStockGetItemIDs = String.format(TPCCConfig.QUERY_STOCK_GET_ITEM_IDS, terminalWarehouseID, districtID, o_id);
                QueryStockGetItemIDs rs = (QueryStockGetItemIDs) cafe.readStatement(queryStockGetItemIDs);
                if (rs == null) {
                    throw new RuntimeException("OL_W_ID="+terminalWarehouseID +" OL_D_ID="+districtID+" OL_O_ID="+o_id+" not found!");
                }
                Map<Integer, Set<Integer>> itemIdsMap = rs.getItemIDs();
                 for (Integer ol_o_id: itemIdsMap.keySet()) {
                    itemIds.addAll(itemIdsMap.get(ol_o_id));
                }
                cafe.getStats().incrBy("stock_level_item_ids", itemIds.size());
                
                if (threshold < 10) {
                    System.out.println("Threshold < 10");
                }
                
                if (TPCCConfig.useRangeQC) {
                    String query = String.format(TPCCConfig.QUERY_RANGE_GET_ITEMS,terminalWarehouseID,9,threshold);
                    QueryResult result = cafe.rangeReadStatement(query);
                    QueryResultRangeGetItems rs2 = (QueryResultRangeGetItems) result;
                    
                    Set<Integer> check = new HashSet<>();
                    List<Integer> ids = rs2.getIds();
                    for (int id: ids) {
                        if (check.contains(id)) {
                            System.out.println("Check contains id " + id);
                        } else {
                            check.add(id);
                            if (itemIds.contains(id)) {
                                stock_count++;
                            }
                        }
                    }
                } else {
                    String[] queries = new String[threshold-10];
                    for (int i = 10; i < threshold; ++i) {
                        queries[i-10] = String.format(TPCCConfig.QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD, terminalWarehouseID, i);
                    }   
                    
                    if (queries.length > 0) {
                        QueryResult[] results = cafe.readStatements(queries);
                        Set<Integer> check = new HashSet<>();
                        for (QueryResult result: results) {
                            QueryStockItemsEqualThreshold rs2 = (QueryStockItemsEqualThreshold) result;
                            if (rs2 == null) {
                                throw new RuntimeException("OL_W_ID="+terminalWarehouseID +" S_I_IDS not found!");
                            }
                            List<Integer> ids = rs2.getIds();
                            for (int id: ids) {
                                if (check.contains(id)) {
//                                    System.out.println("Check contains id " + id);
                                } else {
                                    check.add(id);
                                    if (itemIds.contains(id)) {
                                        stock_count++;
                                    }
                                }
                            }
                        }
                    }
                }

                if (cafe.validateSession()) {
                    conn.commit();
                    cafe.commitSession();
                } else {
                    conn.rollback();
                    cafe.abortSession();
                }

                StringBuilder terminalMessage = new StringBuilder();
                terminalMessage
                .append("\n+-------------------------- STOCK-LEVEL --------------------------+");
                terminalMessage.append("\n Warehouse: ");
                terminalMessage.append(terminalWarehouseID);
                terminalMessage.append("\n District:  ");
                terminalMessage.append(districtID);
                terminalMessage.append("\n\n Stock Level Threshold: ");
                terminalMessage.append(threshold);
                terminalMessage.append("\n Low Stock Count:       ");
                terminalMessage.append(stock_count);
                terminalMessage
                .append("\n+-----------------------------------------------------------------+\n\n");
                if(LOG.isTraceEnabled())LOG.trace(terminalMessage.toString());

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
        
        if (Config.ENABLE_LOGGING) {
            tres.put("w_id", terminalWarehouseID);
            tres.put("d_id", districtID);
            tres.put("T", threshold);
            tres.put("d_next_o_id", o_id);
            tres.put("item_ids", Arrays.toString(itemIds.toArray(new Integer[0])));
            tres.put("stock_count", stock_count);
        }
    }

    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID, NgCache cafe, Map<String, Object> tres)
                    throws SQLException {
        int threshold = TPCCUtil.randomNumber(10, 20, gen);
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        
        if (Config.DEBUG) {
            out.println(String.format("Stock Level d_id=%d, threshold=%d", districtID, threshold));
        }
        
        stockLevelTransaction2(terminalWarehouseID, districtID, threshold, cafe, conn, tres);
        return null;
    }
}
