package com.oltpbenchmark.benchmarks.smallbank.results;

import com.usc.dblab.cafe.QueryResult;

public class SavingsResult extends QueryResult {
	private final double bal;

	public SavingsResult(String query, double bal) {
		super(query);
		this.bal = bal;
	}	
	
	public double getBal() {
		return bal;
	}
}
