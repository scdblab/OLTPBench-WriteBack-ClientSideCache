package com.oltpbenchmark.benchmarks.tpcc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Scanner;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.benchmarks.Config;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;

public class OnlyBackgroundWorker {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Invalid parameters");
            System.exit(-1);
        }
        
        // setup the connection and cache pool 
        String[] cacheips = args[0].split(",");
        String url = "jdbc:mysql://"+args[1]+":3306/tpcc?serverTimezone=UTC&useSSL=false";
        String user = args[2];
        String pass = args[3];
        
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        SockIOPool cacheConnectionPool = SockIOPool.getInstance(TPCCConstants.CACHE_POOL_NAME);
        cacheConnectionPool.setServers(cacheips);
        cacheConnectionPool.setFailover(true);
        cacheConnectionPool.setInitConn(10);
        cacheConnectionPool.setMinConn(5);
        cacheConnectionPool.setMaxConn(200);
        cacheConnectionPool.setNagle(false);
        cacheConnectionPool.setSocketTO(0);
        cacheConnectionPool.setAliveCheck(true);
        cacheConnectionPool.initialize();
        System.out.println("Cache servers: "+cacheips);
        
        CacheStore cacheStore = new TPCCCacheStore(conn);
        TPCCWriteBack writeBack = new TPCCWriteBack(conn, 1);
        
        Stats stats = Stats.getStatsInstance(0);
        
        NgCache cache = new NgCache(cacheStore, writeBack, TPCCConstants.CACHE_POOL_NAME, 
                CachePolicy.WRITE_BACK, 1, stats, url, user, pass, true, 0, 0, 1);
        
        Scanner sc = new Scanner(System.in);
        
        while (true) {
            String str = sc.nextLine();
            if (str.equals("quit")) {
                sc.close();
                cache.clean();                
                System.exit(0);
            }
        }
    }
}
