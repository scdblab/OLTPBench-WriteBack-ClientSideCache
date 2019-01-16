package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetStockCountResult extends QueryResult {
    private int stockCount;
    
    public QueryGetStockCountResult(String query, int stock_count) {
        super(query);
        this.stockCount = stock_count;
    }

    public int getStockCount() {
        return stockCount;
    }

}
