package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetItemResult extends QueryResult {
    final float i_price;
    final String i_name;
    final String i_data;
    
	public QueryGetItemResult(String query, float i_price, String i_name, String i_data) {
		super(query);
		this.i_price = i_price;
		this.i_name = i_name;
		this.i_data = i_data;
	}

	public float getPrice() {
	    return this.i_price;
	}

	public String getName() {
	    return this.i_name;
	}

	public String getData() {
	    return this.i_data;
	}

}
