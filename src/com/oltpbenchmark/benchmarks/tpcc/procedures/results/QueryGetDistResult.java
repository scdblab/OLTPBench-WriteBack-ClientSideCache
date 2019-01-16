package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetDistResult extends QueryResult {
    private final int d_next_o_id;
    private final float d_tax;
    
	public QueryGetDistResult(String query, int d_next_o_id, float d_tax) {
		super(query);
		this.d_next_o_id = d_next_o_id;
		this.d_tax = d_tax;
	}

	public int getNextOrderId() {
	    return d_next_o_id;
	}

	public float getTax() {
	    return d_tax;
	}
}
