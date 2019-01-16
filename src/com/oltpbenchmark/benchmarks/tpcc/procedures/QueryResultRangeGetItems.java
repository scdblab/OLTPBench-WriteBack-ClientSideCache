package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.util.List;

import com.usc.dblab.cafe.QueryResult;

public class QueryResultRangeGetItems extends QueryResult {
    List<Integer> ids;

    public QueryResultRangeGetItems(String query, List<Integer> ids) {
        super(query);
        this.ids = ids;
    }

    public List<Integer> getIds() {
        return ids;
    }

}
