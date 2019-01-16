package com.oltpbenchmark.benchmarks.smallbank.results;

import com.usc.dblab.cafe.QueryResult;

public class AccountResult extends QueryResult {
	private final long custId;
	private final String name;
	
	public AccountResult(String query, long custId, String name) {
		super(query);
		
		this.custId = custId;
		this.name = name;
	}

	public long getCustId() {
		return custId;
	}

	public String getName() {
		return name;
	}

}
