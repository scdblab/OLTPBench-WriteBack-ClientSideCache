package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.tpcc.TPCCCacheStore;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWriteBack;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustCDataResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustIdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustWhseResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustomerById;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetDist2Result;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetDistResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetItemResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetOrderIdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetStockResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetSumOrderAmountResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetWhseResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryOrdStatGetNewestOrdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryOrderStatGetOrderLinesResult;
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;

public class ReadOnly extends TPCCProcedure {
//    static Random rand = new Random();
//    final static Connection[] conns = new Connection[10];
//    final static NgCache[] ngCaches = new NgCache[10];
    
    static int distperwhse;
    static int custperdist;
    
    boolean aDistPerThread = true;
    int distId = 0;

    public ReadOnly(int districtId, boolean aDistPerThread) {
        this.distId = districtId;
        this.aDistPerThread = aDistPerThread;
    }

    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID,
            int numWarehouses, int terminalDistrictLowerID,
            int terminalDistrictUpperID, Map<String, Object> tres)
                    throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet run(Connection conn, Random gen, int w_id,
            int numWarehouses, int terminalDistrictLowerID,
            int terminalDistrictUpperID, NgCache cafe, Map<String, Object> tres)
                    throws SQLException {
        
        if (aDistPerThread) {
            terminalDistrictLowerID = distId;
            terminalDistrictUpperID = distId;
        }
        
        if (TPCCConfig.useRangeQC) {
            try {
                cafe.startSession(null);
                String query = String.format(TPCCConfig.QUERY_RANGE_GET_ITEMS,w_id,10,20);
                cafe.rangeReadStatement(query);
                cafe.commitSession();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            } 
            System.out.println("Completed QUERY_RANGE_GET_ITEMS.");
        }

        // cache QUERY_GET_CUST_WHSE
        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            for (int c_id = 1; c_id <= custperdist; ++c_id) {
                try {
                    cafe.startSession(null);

                    String queryGetCustWhse = String.format(TPCCConfig.QUERY_GET_CUST_WHSE, w_id, d_id, c_id);
                    QueryGetCustWhseResult res1 = (QueryGetCustWhseResult)cafe.readStatement(queryGetCustWhse);
                    if (res1 == null)
                        throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");

                    cafe.commitSession();
                } catch (Exception e) {    
                    e.printStackTrace(System.out);
                    try {
                        cafe.abortSession();
                    } catch (Exception e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }                   
            }
        }
        System.out.println("Cache QUERY_GET_CUST_WHSE done.");        

        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            String queryGetDist = String.format(TPCCConfig.QUERY_DISTRICT_NEXT_ORDER, w_id, d_id);
            QueryGetDistResult res2;
            try {
                cafe.startSession(null);

                res2 = (QueryGetDistResult) cafe.readStatement(queryGetDist);
                if (res2 == null) {
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                }
                int o_id = res2.getNextOrderId();
                
                String queryStockGetItemIDs = String.format(TPCCConfig.QUERY_STOCK_GET_ITEM_IDS, w_id, d_id, o_id);
                QueryStockGetItemIDs rs = (QueryStockGetItemIDs) cafe.readStatement(queryStockGetItemIDs);
                if (rs == null) {
                    throw new RuntimeException("OL_W_ID="+w_id +" OL_D_ID="+d_id+" OL_O_ID="+o_id+" not found!");
                }

                cafe.commitSession();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            } 
        }
        System.out.println("Cache QUERY_DISTRICT_NEXT_ORDER done.");

        int start = 0; // numWarehouses *(w_id-1)+1;
        int end = 100000; // numWarehouses * w_id;

        for (int i_id = start; i_id <= end; ++i_id) {
            String queryGetItem = String.format(TPCCConfig.QUERY_GET_ITEM, i_id);
            try {
                cafe.startSession(null);
                QueryGetItemResult res3 = (QueryGetItemResult) cafe.readStatement(queryGetItem);
                cafe.commitSession();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }
        System.out.println("Cache QUERY_GET_ITEM done.");

        for (int i_id = 1; i_id <= 100000; ++i_id) {
            String queryGetStock = String.format(TPCCConfig.QUERY_GET_STOCK, w_id, i_id);
            QueryGetStockResult res4;
            try {
                cafe.startSession(null);
                res4 = (QueryGetStockResult) cafe.readStatement(queryGetStock);
                if (res4 == null)
                    throw new RuntimeException("I_ID=" + i_id + " not found!");
                cafe.commitSession();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }
        System.out.println("Cache QUERY_GET_STOCK done.");
        
//        System.exit(-1);

        String queryGetWhse = String.format(TPCCConfig.QUERY_GET_WHSE, w_id);
        QueryGetWhseResult res1;
        try {
            cafe.startSession(null);
            res1 = (QueryGetWhseResult) cafe.readStatement(queryGetWhse);
            if (res1 == null)
                throw new RuntimeException("W_ID=" + w_id + " not found!");
            cafe.commitSession();
        } catch (Exception e) {
            e.printStackTrace(System.out);
            try {
                cafe.abortSession();
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        System.out.println("Cache QUERY_GET_WHSE done.");

        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            String getDist = String.format(TPCCConfig.QUERY_GET_DIST2, w_id, d_id);
            QueryGetDist2Result res2;
            try {
                cafe.startSession(null);
                res2 = (QueryGetDist2Result) cafe.readStatement(getDist);
                if (res2 == null)
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!"); 
                cafe.commitSession();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }               
        }
        System.out.println("Cache QUERY_GET_DIST2 done.");

        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            for (int c_id = 1; c_id <= custperdist; ++c_id) {
                try {
                    cafe.startSession(null);
                    String queryPayGetCust = String.format(TPCCConfig.QUERY_PAY_GET_CUST, w_id, d_id, c_id);
                    QueryGetCustomerById rs = (QueryGetCustomerById) cafe.readStatement(queryPayGetCust);
                    if (rs == null) {
                        throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + d_id
                                + " C_W_ID=" + w_id + " not found!");
                    }
                    cafe.commitSession();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    try {
                        cafe.abortSession();
                    } catch (Exception e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }               
            }
        }
        System.out.println("Cache QUERY_PAY_GET_CUST done.");
        
        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            for (int num = 0; num <= 999; ++num) {
                String lastName = TPCCUtil.getLastName(num);
                
                try {
                    cafe.startSession(null);

                    String queryGetCustByName = String.format(TPCCConfig.QUERY_PAY_GET_CUST_BY_NAME, w_id, d_id, lastName);
                    cafe.readStatement(queryGetCustByName);
                    cafe.commitSession();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    try {
                        cafe.abortSession();
                    } catch (Exception e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }             
            }
        }
        System.out.println("Cache QUERY_PAY_GET_CUST_BY_NAME done.");

        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            for (int c_id = 1; c_id <= custperdist; ++c_id) {
                try {
                    cafe.startSession(null);
                    String queryGetCustCdata = String.format(TPCCConfig.QUERY_GET_CUST_C_DATA, w_id, d_id, c_id);
                    QueryGetCustCDataResult res3 = (QueryGetCustCDataResult)cafe.readStatement(queryGetCustCdata);
                    if (res3 == null)
                        throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
                    cafe.commitSession();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    try {
                        cafe.abortSession();
                    } catch (Exception e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }               
            }
        }
        System.out.println("Cache QUERY_GET_CUST_C_DATA done.");

        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            for (int o_id = 1; o_id <= 3000; ++o_id) {
                try {
                    cafe.startSession(null);
                    String queryOrdStatGetOrderLines = String.format(TPCCConfig.QUERY_ORDER_STAT_GET_ORDER_LINES, w_id, d_id, o_id);
                    QueryOrderStatGetOrderLinesResult res = (QueryOrderStatGetOrderLinesResult) cafe.readStatement(queryOrdStatGetOrderLines);
                    cafe.commitSession();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    try {
                        cafe.abortSession();
                    } catch (Exception e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }               
            }
        }

        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            for (int c_id = 1; c_id <= custperdist; ++c_id) {
                try {
                    cafe.startSession(null);
                    String queryOrdStatGetNewestOrd = String.format(TPCCConfig.QUERY_ORDER_STAT_GET_NEWEST_ORDER, w_id, d_id, c_id);
                    QueryOrdStatGetNewestOrdResult rs = (QueryOrdStatGetNewestOrdResult) cafe.readStatement(queryOrdStatGetNewestOrd);
                    if (rs == null) {
                        throw new RuntimeException("No orders for O_W_ID=" + w_id
                                + " O_D_ID=" + d_id + " O_C_ID=" + c_id);
                    }
                    cafe.commitSession();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    try {
                        cafe.abortSession();
                    } catch (Exception e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }               
            }
        }

        int[] ids = new int[terminalDistrictUpperID-terminalDistrictLowerID+1];
        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            try {
                cafe.startSession(null);
                String getOrderId = String.format(TPCCConfig.QUERY_GET_ORDER_ID, w_id, d_id);
                QueryGetOrderIdResult resX = (QueryGetOrderIdResult) cafe.readStatement(getOrderId);
                int no_o_id = -1;
                if (resX == null) {
                    // This district has no new orders; this can happen but should
                    // be rare
                    if (Config.ENABLE_LOGGING) {
                        tres.put("o" + d_id, no_o_id);
                    }
                    continue;
                }

                ids[d_id-terminalDistrictLowerID] = resX.getNewOrderIds().get(0);

                cafe.commitSession();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }   
        }

        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            for (int no_o_id = ids[d_id-terminalDistrictLowerID]; no_o_id <= 3000; ++no_o_id) {
                try {
                    cafe.startSession(null);
                    
                    String getCustId = String.format(TPCCConfig.QUERY_DELIVERY_GET_CUST_ID, w_id, d_id, no_o_id);
                    QueryGetCustIdResult res2 = (QueryGetCustIdResult) cafe.readStatement(getCustId);
                    
                    String queryStockGetItemIDs = String.format(TPCCConfig.QUERY_STOCK_GET_ITEM_IDS, w_id, d_id, no_o_id);
                    QueryStockGetItemIDs rs = (QueryStockGetItemIDs) cafe.readStatement(queryStockGetItemIDs);
    
                    String getSumOrderAmount = String.format(TPCCConfig.QUERY_GET_SUM_ORDER_AMOUNT, w_id, d_id, no_o_id);
                    QueryGetSumOrderAmountResult res3 = (QueryGetSumOrderAmountResult) cafe.readStatement(getSumOrderAmount);
                    
                    cafe.commitSession();
                    
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    try {
                        cafe.abortSession();
                    } catch (Exception e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }   
            }
        }

        for (int d_id = terminalDistrictLowerID; d_id <= terminalDistrictUpperID; ++d_id) {
            try {
                cafe.startSession(null);
                String getOrderId = String.format(TPCCConfig.QUERY_GET_ORDER_ID, w_id, d_id);
                cafe.readStatement(getOrderId);              
                
                cafe.commitSession();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }   
        }
        
        for (int i = 10; i <= 20; ++i) {
            try {
                cafe.startSession(null);
                String query = String.format(TPCCConfig.QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD, w_id, i);
                cafe.readStatement(query);
                cafe.commitSession();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }            
        }

        return null;
    }

    public static void main(String[] args) {
        Config.DEBUG = false;

        int scale = Integer.parseInt(args[0]);
        String[] cacheServers = args[1].split(",");
        String dbip = args[2];
        String dbname = args[3];
        String dbpass = args[4];
        int minw = Integer.parseInt(args[5]);
        int maxw = Integer.parseInt(args[6]);        
        
        distperwhse = Integer.parseInt(args[7]);
        custperdist = Integer.parseInt(args[8]);
        boolean aDistPerThread = Boolean.parseBoolean(args[9]);
        
        System.out.println("Scale = "+scale);

        SockIOPool cacheConnectionPool = SockIOPool.getInstance(Config.CACHE_POOL_NAME);
        cacheConnectionPool.setServers(cacheServers);
        cacheConnectionPool.setFailover(true);
        cacheConnectionPool.setInitConn(10);
        cacheConnectionPool.setMinConn(5);
        cacheConnectionPool.setMaxConn(200);
        cacheConnectionPool.setNagle(false);
        cacheConnectionPool.setSocketTO(0);
        cacheConnectionPool.setAliveCheck(true);
        cacheConnectionPool.initialize();
        System.out.println("Cache servers: "+Arrays.toString(cacheServers));
        
        com.usc.dblab.cafe.Config.storeCommitedSessions = false;

        LoadThread[] threads = null;
        if (!aDistPerThread) {
            threads = new LoadThread[maxw-minw+1];  
            for (int i = minw; i <= maxw; i++) {
                threads[i-minw] = new LoadThread(scale, i, dbip, dbname, dbpass, 
                        distperwhse, custperdist, 0, false, i-minw);
                threads[i-minw].start();
            }
        } else {
            threads = new LoadThread[(maxw-minw+1)*distperwhse];
            for (int i = minw; i <= maxw; i++) {
                for (int j = 1; j <= distperwhse; j++) {
                    threads[(i-minw)*distperwhse+(j-1)] = new LoadThread(scale, i, dbip, 
                            dbname, dbpass, distperwhse, custperdist, j, true, (i-minw)*distperwhse+(j-1));
                    threads[(i-minw)*distperwhse+(j-1)].start();
                }
            }
        }

        for (int i = 1; i <= threads.length; i++) {
            try {
                threads[i-1].join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        System.out.println(Stats.getAllStats().toString(4));

        System.out.println("Loading completed.");
    }
}

class LoadThread extends Thread {
//    static Random rand = new Random();
    int warehouseId;
    int scale;
    int distperwhse;
    int custperdist;
    boolean aDistPerThread = true;
    int districtId = 0;
    int threadId = 0;

    String dbip, dbname, dbpass;

    public LoadThread(int scale, int warehouseId, String dbip, String dbname, 
            String dbpass, int distperwhse, int custperdist, 
            int districtId, boolean aDistPerThread, int threadId) {
        this.scale = scale;
        this.warehouseId = warehouseId;
        this.dbip = dbip;
        this.dbname = dbname;
        this.dbpass = dbpass;
        this.distperwhse = distperwhse;
        this.custperdist = custperdist;
        this.aDistPerThread = aDistPerThread;
        this.districtId = districtId;
        this.threadId = threadId;
    }

    @Override
    public void run() {
        if (aDistPerThread) {
            System.out.println("Start loading for warehouse "+warehouseId+", district "+districtId);
        } else {
            System.out.println("Start loading for warehouse "+warehouseId);
        }

        Connection conn = null;
        try {
            if (!dbip.contains("jdbc")) {
                conn = DriverManager.getConnection(
                    "jdbc:mysql://"+dbip+":3306/tpcc?serverTimezone=UTC&useSSL=false", 
                    dbname, dbpass);
            } else {
                conn = DriverManager.getConnection(
                        dbip, dbname, dbpass);
            }
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        CacheStore cacheStore = new TPCCCacheStore(conn);
        WriteBack cacheBack = new TPCCWriteBack(conn, warehouseId);

        NgCache cache = null;
        if (dbip.contains("jdbc")) {
            cache = new NgCache(cacheStore, cacheBack, 
                    Config.CACHE_POOL_NAME, CachePolicy.WRITE_THROUGH, 0, Stats.getStatsInstance(threadId),
                    dbip, dbname, dbpass, false, 0, 0, 0);
        } else {
            cache = new NgCache(cacheStore, cacheBack, 
                    Config.CACHE_POOL_NAME, CachePolicy.WRITE_THROUGH, 0, Stats.getStatsInstance(threadId),
                    "jdbc:mysql://"+dbip+":3306/tpcc?serverTimezone=UTC&useSSL=false", dbname, dbpass, false, 0, 0, 0);
        }

        ReadOnly ro = new ReadOnly(districtId, aDistPerThread);
        Map<String, Object> tres = new HashMap<>();
        try {
            ro.run(conn, null, warehouseId, scale, 1, distperwhse, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        cache.clean();
    }
}
