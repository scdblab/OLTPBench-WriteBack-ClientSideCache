package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import java.util.List;

import com.usc.dblab.cafe.QueryResult;

public class QueryStockItemsEqualThreshold extends QueryResult {
    List<Integer> ids;
    
    public QueryStockItemsEqualThreshold(String query, List<Integer> ids) {
        super(query);
        this.ids = ids;
    }

    public List<Integer> getIds() {
        return ids;
    }
}
