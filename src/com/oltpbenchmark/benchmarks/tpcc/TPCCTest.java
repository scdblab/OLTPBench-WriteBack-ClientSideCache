package com.oltpbenchmark.benchmarks.tpcc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Payment;
import com.oltpbenchmark.benchmarks.tpcc.procedures.ReadOnly;
import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;

public class TPCCTest {
    static StockLevel stockLevel = new StockLevel();
    static OrderStatus orderStatus = new OrderStatus();
    static NewOrder newOrder = new NewOrder();
    static Payment payment = new Payment();
    static Delivery delivery = new Delivery();

    static SockIOPool cacheConnectionPool;
    static Connection conn;
    static NgCache cache;
    static Random rand = new Random();

    public static void main(String[] args) {
        Config.DEBUG = true;
        Config.ENABLE_LOGGING = true;

        cacheConnectionPool = SockIOPool.getInstance(Config.CACHE_POOL_NAME);
        cacheConnectionPool.setServers(new String[] { "10.0.0.210:11211" });
        cacheConnectionPool.setFailover(true);
        cacheConnectionPool.setInitConn(10);
        cacheConnectionPool.setMinConn(5);
        cacheConnectionPool.setMaxConn(200);
        cacheConnectionPool.setNagle(false);
        cacheConnectionPool.setSocketTO(0);
        cacheConnectionPool.setAliveCheck(true);
        cacheConnectionPool.initialize();

        try {
            conn = DriverManager.getConnection(
                    "jdbc:mysql://10.0.0.220:3306/tpcc?serverTimezone=UTC&useSSL=false", 
                    "hieun", "golinux");
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        String[] as = new String[] { "1", "10.0.0.210:11211", "jdbc:mysql://10.0.0.220:3306/tpcc?serverTimezone=UTC&useSSL=false", "hieun", "golinux", "1", "1", "10", "3000", "true" };
        ReadOnly.main(as);

        CacheStore cacheStore = new TPCCCacheStore(conn);
        WriteBack cacheBack = new TPCCWriteBack(conn, 1);

        cache = new NgCache(cacheStore, cacheBack, 
                Config.CACHE_POOL_NAME, CachePolicy.WRITE_BACK, 0, Stats.getStatsInstance(0),
                "jdbc:mysql://10.0.0.220:3306/tpcc?serverTimezone=UTC&useSSL=false", "hieun", "golinux", true, 0, 0, 0);       

        Map<String, Object> tres = new HashMap<>();
        
        while (true) {
            verifyNewOrder(tres);
            tres.clear();
        }
//        verifyNewOrder(tres);
//        verifyDelivery(tres);
//        verifyNewOrder(tres);
    }

    private static void verifyStockLevel(Map<String, Object> tres) {
        try {
            stockLevel.run(conn, rand, 1, 1, 1, 10, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static void verifyStockLevel2(Map<String, Object> tres) {
        int threshold = 15;
        int districtID = 5;
        
        stockLevel.stockLevelTransaction(1, districtID, threshold, cache, conn, tres);
        
        int customerID = TPCCUtil.getCustomerID(rand);

        int numItems = (int) TPCCUtil.randomNumber(5, 15, rand);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TPCCUtil.getItemID(rand);
            if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                supplierWarehouseIDs[i] = 1;
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
        }      
        
        newOrder.newOrderTransaction2(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, conn, cache, tres);
        
        stockLevel.stockLevelTransaction(1, districtID, threshold, cache, conn, tres);
    }

    private static void verifyOrderStatus(Map<String, Object> tres) {
        try {
            orderStatus.run(conn, rand, 1, 1, 1, 10, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void verifyPayment(Map<String, Object> tres) {
        try {
            payment.run(conn, rand, 1, 1, 1, 10, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void verifyNewOrder(Map<String, Object> tres) {
        try {
            int districtID = 1;
            int customerID = TPCCUtil.getCustomerID(rand);

            int numItems = (int) TPCCUtil.randomNumber(5, 15, rand);
            int[] itemIDs = new int[numItems];
            int[] supplierWarehouseIDs = new int[numItems];
            int[] orderQuantities = new int[numItems];
            int allLocal = 1;
            for (int i = 0; i < numItems; i++) {
                itemIDs[i] = TPCCUtil.getItemID(rand);
                if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                    supplierWarehouseIDs[i] = 1;
                } else {
                    do {
                        supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                    } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                    allLocal = 0;
                }
                orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
            }      

            tres.clear();
            newOrder.newOrderTransaction2(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);

//            tres.clear();
//            orderStatus.orderStatusTransaction(1, districtID, customerID, null, false, conn, cache, tres);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void verifyNewOrder2(Map<String, Object> tres) {
        try {
            int districtID = TPCCUtil.randomNumber(1, 10, rand);
            int customerID = TPCCUtil.getCustomerID(rand);

            int numItems = (int) TPCCUtil.randomNumber(5, 15, rand); 
            int[] itemIDs = new int[numItems];
            int[] supplierWarehouseIDs = new int[numItems];
            int[] orderQuantities = new int[numItems];
            int allLocal = 1;
            for (int i = 0; i < numItems; i++) {
                itemIDs[i] = TPCCUtil.getItemID(rand);
                if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                    supplierWarehouseIDs[i] = 1;
                } else {
                    do {
                        supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                    } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                    allLocal = 0;
                }
                orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
            }                            

            //            tres.clear();
            //            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
            //                    conn, cache, tres);

            //            tres.clear();
            //            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
            //                    conn, cache, tres);

            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);
            
            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);            

            //customerID = TPCCUtil.getCustomerID(rand);
            tres.clear();
            newOrder.newOrderTransaction2(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);

            tres.clear();
            orderStatus.orderStatusTransaction(1, districtID, customerID, null, false, conn, cache, tres);

            customerID = TPCCUtil.getCustomerID(rand);
            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);

            customerID = TPCCUtil.getCustomerID(rand);
            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);

            tres.clear();
            orderStatus.orderStatusTransaction(1, districtID, customerID, null, false, conn, cache, tres);

            //            tres.clear();
            //            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
            //                    conn, cache, tres);            

            //            tres.clear();
            //            orderStatus.orderStatusTransaction(1, districtID, customerID, null, false, conn, cache, tres);            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static void verifyNewOrder3(Map<String, Object> tres) {
        try {
            int districtID = TPCCUtil.randomNumber(1, 10, rand);
            int customerID = TPCCUtil.getCustomerID(rand);

            int numItems = (int) TPCCUtil.randomNumber(5, 15, rand);
            int[] itemIDs = new int[numItems];
            int[] supplierWarehouseIDs = new int[numItems];
            int[] orderQuantities = new int[numItems];
            int allLocal = 1;
            for (int i = 0; i < numItems; i++) {
                itemIDs[i] = TPCCUtil.getItemID(rand);
                if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                    supplierWarehouseIDs[i] = 1;
                } else {
                    do {
                        supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                    } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                    allLocal = 0;
                }
                orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
            }              

            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);
            
            tres.clear();
            orderStatus.orderStatusTransaction(1, districtID, customerID, null, false, conn, cache, tres);  
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static void verifyNewOrderStockLevel(Map<String, Object> tres) {
        try {
            int districtID = TPCCUtil.randomNumber(1, 10, rand);
            int customerID = TPCCUtil.getCustomerID(rand);

            int numItems = (int) TPCCUtil.randomNumber(5, 15, rand);
            int[] itemIDs = new int[numItems];
            int[] supplierWarehouseIDs = new int[numItems];
            int[] orderQuantities = new int[numItems];
            int allLocal = 1;
            for (int i = 0; i < numItems; i++) {
                itemIDs[i] = TPCCUtil.getItemID(rand);
                if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                    supplierWarehouseIDs[i] = 1;
                } else {
                    do {
                        supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                    } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                    allLocal = 0;
                }
                orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
            }              

            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);
            
            tres.clear();
            delivery.deliveryTransaction(1, 300, conn, cache, tres);  
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static void verifyOrderStatus2(Map<String, Object> tres) {
        try {
            int districtID = TPCCUtil.randomNumber(1, 10, rand);
            int customerID = TPCCUtil.getCustomerID(rand);

            int numItems = (int) TPCCUtil.randomNumber(5, 15, rand);
            int[] itemIDs = new int[numItems];
            int[] supplierWarehouseIDs = new int[numItems];
            int[] orderQuantities = new int[numItems];
            int allLocal = 1;
            for (int i = 0; i < numItems; i++) {
                itemIDs[i] = TPCCUtil.getItemID(rand);
                if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                    supplierWarehouseIDs[i] = 1;
                } else {
                    do {
                        supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                    } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                    allLocal = 0;
                }
                orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
            }              
            
            tres.clear();
            orderStatus.orderStatusTransaction(1, districtID, customerID, null, false, conn, cache, tres);
            
            tres.clear();
            orderStatus.orderStatusTransaction(1, districtID, customerID, null, false, conn, cache, tres);
            
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static void verifyDelivery2(Map<String, Object> tres) {
        try {
            int districtID = TPCCUtil.randomNumber(1, 10, rand);
            int customerID = TPCCUtil.getCustomerID(rand);

            int numItems = 1;  
            int[] itemIDs = new int[numItems];
            int[] supplierWarehouseIDs = new int[numItems];
            int[] orderQuantities = new int[numItems];
            int allLocal = 1;
            for (int i = 0; i < numItems; i++) {
                itemIDs[i] = TPCCUtil.getItemID(rand);
                if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                    supplierWarehouseIDs[i] = 1;
                } else {
                    do {
                        supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                    } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                    allLocal = 0;
                }
                orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
            }                            

//            for (int i = 1; i < 10; ++i) {
//                customerID = TPCCUtil.getCustomerID(rand);
//                itemIDs[0] = TPCCUtil.getItemID(rand);
//                tres.clear();
//                newOrder.newOrderTransaction(1, i, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
//                    conn, cache, tres);
//            }
            
            int orderCarrierID = TPCCUtil.randomNumber(1, 10, rand);
            tres.clear();
            delivery.deliveryTransaction(1, orderCarrierID, conn, cache, tres);
            
            orderCarrierID = TPCCUtil.randomNumber(1, 10, rand);
            tres.clear();
            delivery.deliveryTransaction(1, orderCarrierID, conn, cache, tres);
            
            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void verifyApplyBufferedWriteNewOrderSameDistrict(Map<String, Object> tres) {
        try {
            int districtID = TPCCUtil.randomNumber(1, 10, rand);
            int customerID = TPCCUtil.getCustomerID(rand);

            int numItems = (int) TPCCUtil.randomNumber(1, 1, rand);
            int[] itemIDs = new int[numItems];
            int[] supplierWarehouseIDs = new int[numItems];
            int[] orderQuantities = new int[numItems];
            int allLocal = 1;
            for (int i = 0; i < numItems; i++) {
                itemIDs[i] = TPCCUtil.getItemID(rand);
                if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                    supplierWarehouseIDs[i] = 1;
                } else {
                    do {
                        supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                    } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                    allLocal = 0;
                }
                orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
            }                          

            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);               

            tres.clear();
            //            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
            //                    conn, cache, tres);
            //            stockLevel.stockLevelTransaction(1, districtID, 10, cache, conn);

            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void verifyDelivery(Map<String, Object> tres) {
        try {
            delivery.run(conn, rand, 1, 1, 1, 10, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }    
    
    private static void verifyDelivery3(Map<String, Object> tres) {

        try {
            int districtID = TPCCUtil.randomNumber(1, 10, rand);
            int customerID = TPCCUtil.getCustomerID(rand);

            int numItems = (int) TPCCUtil.randomNumber(5, 15, rand);
            int[] itemIDs = new int[numItems];
            int[] supplierWarehouseIDs = new int[numItems];
            int[] orderQuantities = new int[numItems];
            int allLocal = 1;
            for (int i = 0; i < numItems; i++) {
                itemIDs[i] = TPCCUtil.getItemID(rand);
                if (TPCCUtil.randomNumber(1, 100, rand) > 1) {
                    supplierWarehouseIDs[i] = 1;
                } else {
                    do {
                        supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, 1, rand);
                    } while (supplierWarehouseIDs[i] == 1 && 1 > 1);
                    allLocal = 0;
                }
                orderQuantities[i] = TPCCUtil.randomNumber(1, 10, rand);
            }      

            tres.clear();
            newOrder.newOrderTransaction(1, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, 
                    conn, cache, tres);

            tres.clear();
            delivery.deliveryTransaction(1, 115, conn, cache, tres);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static void verifyDelivery4(Map<String, Object> tres) {

        try {
            tres.clear();
            delivery.deliveryTransaction(1, 115, conn, cache, tres);            
            
            tres.clear();
            orderStatus.orderStatusTransaction(1, 1, 2666, null, false, conn, cache, tres);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
