package com.oltpbenchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.oltpbenchmark.benchmarks.Config;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;

public class MonitorThread extends Thread {
    long old_hits = 0;
    long old_misses = 0;
    static final int WINDOW = 1000;
    static volatile boolean stop = false;
    public static double warmupThreshold = 0.9;
    
    Connection conn;
    Statement stmt;
    
    public MonitorThread(String dburl, String dbuser, String dbpass) {
        this.old_hits = 0;
        this.old_misses = 0;

        try {
            conn = DriverManager.getConnection(dburl, dbuser, dbpass);
            conn.setAutoCommit(true);
            
            stmt = conn.createStatement();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @Override
    public void run() {
        boolean switchMode = false;
        long sleepTime = 1000;
        
//        if (Config.CACHE_POLICY == CachePolicy.WRITE_BACK)
//            sleepTime = 10000;
        
        System.out.println("Monitor thread is running, mix threshold = "+warmupThreshold);
        
        while (!stop) {
            long hits = Stats.get(Stats.METRIC_CACHE_HITS);
            long misses = Stats.get(Stats.METRIC_CACHE_MISSES);
            long diff = (hits+misses) - (old_hits+old_misses);
            
            if (diff > WINDOW) {
                long window_hits = hits - old_hits;
                long window_misses = misses - old_misses;
                
                double rate = 0.0;
                if (window_hits + window_misses > 0) {
                    rate = (double) window_hits / (window_hits + window_misses);
                }
                
                System.out.println("Window cache hit rate "+rate);
                
                // query
                printNumberOfSessionRows();
                
                if (rate >= warmupThreshold && !switchMode && Config.CACHE_POLICY == CachePolicy.WRITE_THROUGH) {
                    Config.CACHE_POLICY = CachePolicy.WRITE_BACK;
                    NgCache.policy = CachePolicy.WRITE_BACK;
                    switchMode = true;
                    sleepTime = 10000;
                    System.out.println("Switch to WRITE_BACK Policy.");
                }
                
                // update hits and misses
                old_hits = hits;
                old_misses = misses;
            }
            
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void printNumberOfSessionRows() {
        try {
            ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM COMMITED_SESSION");
            res.next();
            int rows = res.getInt(1);
            res.close();
            
            System.out.println("# Rows CommitedSession table: "+rows);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
