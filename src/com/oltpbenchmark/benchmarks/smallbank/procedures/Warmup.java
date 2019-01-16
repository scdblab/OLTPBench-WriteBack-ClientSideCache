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

public class Warmup {    
    static SockIOPool cacheConnectionPool;
    
    static SmallBankBenchmark bench;
    static SmallBankWorker worker;
    static Random rand = new Random();
    static final int DB_SIZE = 1000000;
    
    public static void main(String[] args) {    
        String dbip = args[0];
        
        String[] caches = null;
        if (args.length >=2)
            caches = args[1].split(",");
        
        if (caches != null) {
            cacheConnectionPool = SockIOPool.getInstance(Config.CACHE_POOL_NAME);
            cacheConnectionPool.setServers(caches);
            cacheConnectionPool.setFailover(true);
            cacheConnectionPool.setInitConn(10);
            cacheConnectionPool.setMinConn(5);
            cacheConnectionPool.setMaxConn(200);
            cacheConnectionPool.setNagle(false);
            cacheConnectionPool.setSocketTO(0);
            cacheConnectionPool.setAliveCheck(true);
            cacheConnectionPool.initialize();
            System.out.println("Cache servers: "+Arrays.toString(caches));
        } else {
            System.out.println("No cache is provided.");
        }
        
        int nthreads = 100;
        int perThread = DB_SIZE / nthreads;
        WarmupThread[] threads = new WarmupThread[nthreads];
        for (int i = 0; i < nthreads; ++i) {
            int st = i*perThread;
            int end = (i+1)*perThread;
            threads[i] = new WarmupThread(st, end, dbip, caches);
            threads[i].start();
        }
        
        for (int i = 0; i < nthreads; ++i) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
   
}

class WarmupThread extends Thread {
    private Connection conn = null;
    private NgCache cache = null;
    int start, end;
    
    Balance procBalance = new Balance();
    GetAccount procGetAcct = new GetAccount();

    
    public WarmupThread(int start, int end, String dbip, String[] caches) {
        try {
            conn = DriverManager.getConnection(
                    "jdbc:mysql://"+dbip+":3306/smallbank?serverTimezone=UTC", 
                    "hieun", "golinux");
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        if (caches != null) {
            CacheStore cacheStore = new SmallBankCacheStore(conn);
            WriteBack cacheBack = new SmallBankWriteBack(conn);
            
            cache = new NgCache(cacheStore, cacheBack, 
                    Config.CACHE_POOL_NAME, CachePolicy.WRITE_THROUGH, 0, Stats.getStatsInstance(0),"jdbc:mysql://"+dbip+":3306/smallbank?serverTimezone=UTC", 
                    "hieun", "golinux", false, 0, 0, 1);
        }
        
        this.start = start;
        this.end = end;
    }
    
    @Override
    public void run() {
        Map<String, Object> tres = new HashMap<>();
        for (int i = start; i < end; i++) {
            if (i % 10000 == 0) {
                System.out.println("Warmup "+i+"...");
            }
            
            try {
                if (cache == null) {
                    procBalance.run(conn, getName(i), tres);
                    procGetAcct.run(conn, i, tres);
                } else {
                    procBalance.run(conn, getName(i), cache, tres);
                    procGetAcct.run(conn, i, cache, tres);
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                conn.commit();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            tres.clear();
        }        
    }
    
    private static String getName(long id) {
        String name = id+"";
        int cnt = 0;
        
        if (id > 0) {
            while (id > 0) {
                id /= 10;
                cnt++;
            }
        } else {
            cnt = 1;
        }
        
        for (int i = 0; i < 64-cnt; ++i) {
            name = "0"+name;
        }
        
        return name;
    }
}
