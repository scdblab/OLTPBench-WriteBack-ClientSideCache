package com.oltpbenchmark.benchmarks;

import com.usc.dblab.cafe.CachePolicy;

public class Config {
    public static final String CACHE_POOL_NAME = "oltpbench";
    public static boolean ENABLE_LOGGING = false;
    public static boolean CAFE = false;
    public static String[] cacheServers = new String[] { "localhost:11211" };

    public static boolean DEBUG = false;
    public static volatile CachePolicy CACHE_POLICY = CachePolicy.WRITE_THROUGH;
    public static int NUM_AR_WORKERS = 0;
    public static int BATCH = 10;
    public static int AR_SLEEP = 0; 
}
