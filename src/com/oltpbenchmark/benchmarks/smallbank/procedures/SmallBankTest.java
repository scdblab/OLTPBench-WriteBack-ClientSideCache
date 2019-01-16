package com.oltpbenchmark.benchmarks.smallbank.procedures;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankBenchmark;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankCacheStore;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWorker;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWriteBack;
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;

public class SmallBankTest {
    static Balance procBalance = new Balance();
    static Amalgamate procAmalgamate = new Amalgamate();
    static DepositChecking procDepositChecking = new DepositChecking();
    static SendPayment procSendPayment = new SendPayment();
    static TransactSavings procTransactSavings = new TransactSavings();
    static WriteCheck procWriteCheck = new WriteCheck();
    static SockIOPool cacheConnectionPool;
    static Connection conn;
    static NgCache cache;
    
    static SmallBankBenchmark bench;
    static SmallBankWorker worker;
    static Random rand = new Random();
    static final int DB_SIZE = 1000000;
    
    public static void main(String[] args) {    
        Config.DEBUG = true;
        
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
        System.out.println("Cache servers: "+Arrays.toString(Config.cacheServers));
        
        try {
            conn = DriverManager.getConnection(
                    "jdbc:mysql://10.0.0.220:3306/smallbank?serverTimezone=UTC", 
                    "hieun", "golinux");
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        CacheStore cacheStore = new SmallBankCacheStore(conn);
        WriteBack cacheBack = new SmallBankWriteBack(conn);
        
        cache = new NgCache(cacheStore, cacheBack, 
                Config.CACHE_POOL_NAME, CachePolicy.WRITE_BACK, 0, Stats.getStatsInstance(0), "jdbc:mysql://10.0.0.220:3306/smallbank?serverTimezone=UTC", 
                "hieun", "golinux", false, 0, 0, 1); 
        
        System.out.println(getName(1));
        System.out.println(getName(322));
        System.out.println(getName(1123));
        
        System.out.println("====== Verify DepositChecking");
        verifyDepositChecking();
        System.out.println("====== Verify WriteCheck");
        verifyWriteCheck();
        System.out.println("====== Verify TransactSavings");
        verifyTransactSavings();
        
        System.out.println("====== Verify Amalgamate");
        verifyAmalgamate(false);
        System.out.println("-------------------------");
        verifyAmalgamate(true);
        
        System.out.println("====== Verify Send Payment");
        verifySendPayment(false);
        System.out.println("-------------------------");
        verifySendPayment(true);     
    }
    
    public static void verifyCacheHit() {
        try {
            for (int i = 0; i < 10; i++) {
                procBalance.run(conn, "0000000000000000000000000000000000000000000000000000000000051991", cache, null);
            }
            System.out.println(Stats.getAllStats().toString(2));
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void verifyDepositChecking() {
        Map<String, Object> tres = new HashMap<String, Object>();
        long id = rand.nextInt(DB_SIZE);
        String name = getName(id);
        try {
            // on cache misses
            procBalance.run(conn, name, tres);
            
            tres.clear();
            procDepositChecking.run(conn, name, 0.25, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
            
            // on cache hits
            tres.clear();
            procDepositChecking.run(conn, name, 0.77, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void verifyWriteCheck() {
        Map<String, Object> tres = new HashMap<String, Object>();
        long id = rand.nextInt(DB_SIZE);
        String name = getName(id);
        try {
            // on cache misses
            procBalance.run(conn, name, tres);
            
            tres.clear();
            procWriteCheck.run(conn, name, 0.25, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
            
            // on cache hits
            tres.clear();
            procWriteCheck.run(conn, name, 0.77, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void verifyTransactSavings() {
        Map<String, Object> tres = new HashMap<String, Object>();
        long id = rand.nextInt(DB_SIZE);
        String name = getName(id);
        try {
            // on cache misses
            procBalance.run(conn, name, tres);
            
            tres.clear();
            procTransactSavings.run(conn, name, 0.25, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
            
            // on cache hits
            tres.clear();
            procTransactSavings.run(conn, name, 0.77, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static String getName(long id) {
        String name = id+"";
        int cnt = 0;
        while (id > 0) {
            id /= 10;
            cnt++;
        }
        for (int i = 0; i < 64-cnt; ++i) {
            name = "0"+name;
        }
        return name;
    }

    public static void verifyAmalgamate(boolean cacheHit) {
        Map<String, Object> tres = new HashMap<String, Object>();
        long custId0 = rand.nextInt(DB_SIZE);
        long custId1 = rand.nextInt(DB_SIZE);
        String name1 = getName(custId0);
        String name2 = getName(custId1);
        
        try {
            if (!cacheHit) {
                procBalance.run(conn, name1, tres);            
                tres.clear();
                procBalance.run(conn, name2, tres);
            } else {
                procBalance.run(conn, name1, cache, tres);            
                tres.clear();
                procBalance.run(conn, name2, cache, tres);
            }
            
            tres.clear();
            procAmalgamate.run(conn, custId0, custId1, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name1, cache, tres);
            tres.clear();
            procBalance.run(conn, name2, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void verifySendPayment(boolean cacheHit) {
        Map<String, Object> tres = new HashMap<String, Object>();
        long custId0 = rand.nextInt(DB_SIZE);
        long custId1 = rand.nextInt(DB_SIZE);
        String name1 = getName(custId0);
        String name2 = getName(custId1);
        
        try {
            if (!cacheHit) {
                procBalance.run(conn, name1, tres);            
                tres.clear();
                procBalance.run(conn, name2, tres);
            } else {
                procBalance.run(conn, name1, cache, tres);            
                tres.clear();
                procBalance.run(conn, name2, cache, tres);
            }
            
            tres.clear();
            procSendPayment.run(conn, custId0, custId1, 0.7, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name1, cache, tres);
            tres.clear();
            procBalance.run(conn, name2, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
