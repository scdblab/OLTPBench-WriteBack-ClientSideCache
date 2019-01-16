package com.oltpbenchmark.benchmarks.smallbank;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Amalgamate;

public class SmallBankBenchmark extends BenchmarkModule {

    protected final long numAccounts;
    
    private SockIOPool cacheConnectionPool;
    
    public SmallBankBenchmark(WorkloadConfiguration workConf) {
        super("smallbank", workConf, true);
        this.numAccounts = (int)Math.round(SmallBankConstants.NUM_ACCOUNTS * workConf.getScaleFactor());
        
        if (Config.CAFE) {
        	cacheConnectionPool = SockIOPool.getInstance(Config.CACHE_POOL_NAME);
        	cacheConnectionPool.setServers(Config.cacheServers);
        	cacheConnectionPool.setFailover(true);
        	cacheConnectionPool.setInitConn(10);
        	cacheConnectionPool.setMinConn(5);
        	cacheConnectionPool.setMaxConn(200);
        	cacheConnectionPool.setNagle(false);
        	cacheConnectionPool.setSocketTO(0);
        	cacheConnectionPool.setAliveCheck(true);
        	cacheConnectionPool.initialize();
        	System.out.println("Cache servers: "+Arrays.toString(Config.cacheServers));
        }
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new SmallBankWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader<SmallBankBenchmark> makeLoaderImpl(Connection conn) throws SQLException {
        return new SmallBankLoader(this, conn);
    }

    @Override
    protected Package getProcedurePackageImpl() {
       return Amalgamate.class.getPackage();
    }
    
    
    /**
     * For the given table, return the length of the first VARCHAR attribute
     * @param acctsTbl
     * @return
     */
    public static int getCustomerNameLength(Table acctsTbl) {
        int acctNameLength = -1;
        for (Column col : acctsTbl.getColumns()) {
            if (SQLUtil.isStringType(col.getType())) {
                acctNameLength = col.getSize();
                break;
            }
        } // FOR
        assert(acctNameLength > 0);
        return (acctNameLength);
    }

}