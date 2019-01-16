package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.usc.dblab.cafe.QueryResult;

public class QueryStockGetCountItems extends QueryResult {
    private final int stockCount;
    
    public QueryStockGetCountItems(String query, int stockCount) {
        super(query);
        this.stockCount = stockCount;
    }

    public int getStockCount() {
        return stockCount;
    }

}
