package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import java.util.List;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetOrderIdResult extends QueryResult {
    private final List<Integer> newOrderIds;
    
	public QueryGetOrderIdResult(String query, List<Integer> newOrderIds) {
		super(query);
		this.newOrderIds = newOrderIds;
	}

	public List<Integer> getNewOrderIds() {
	    return newOrderIds;
	}
}
