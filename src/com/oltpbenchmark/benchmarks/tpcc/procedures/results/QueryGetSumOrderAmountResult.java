package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetSumOrderAmountResult extends QueryResult {
    final double ol_total;
    
    public QueryGetSumOrderAmountResult(String query, double ol_total) {
        super(query);
        this.ol_total = ol_total;
    }

    public double getTotal() {
        return ol_total;
    }

}
