package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetCustIdResult extends QueryResult {
    final int c_id;
    
    public QueryGetCustIdResult(String query, int c_id) {
        super(query);
        this.c_id = c_id;
    }

    public int getOCId() {
        return this.c_id;
    }

}
