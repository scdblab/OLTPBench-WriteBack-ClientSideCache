package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.usc.dblab.cafe.QueryResult;

public class QueryStockGetItemIDs extends QueryResult {
    private final Map<Integer, Set<Integer>> itemIds;
    
    public QueryStockGetItemIDs(String query, Map<Integer, Set<Integer>> itemIds) {
        super(query);
        this.itemIds = itemIds;
    }

    public Map<Integer, Set<Integer>> getItemIDs() {
        return itemIds;
    }
}
