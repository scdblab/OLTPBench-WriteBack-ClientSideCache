package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetCustCDataResult extends QueryResult {

    private String c_data;

    public QueryGetCustCDataResult(String query, String c_data) {
        super(query);
        this.c_data = c_data;
    }

    public String getCData() {
        return c_data;
    }

}
