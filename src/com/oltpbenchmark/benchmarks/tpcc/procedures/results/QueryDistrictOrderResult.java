package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryDistrictOrderResult extends QueryResult {
    private int nextOId;
    
    public QueryDistrictOrderResult(String query, int next_o_id) {
        super(query);
        this.nextOId = next_o_id;
    }
    
    public int getDNextOID() {
        return this.nextOId;
    }

}
