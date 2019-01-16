package com.oltpbenchmark.benchmarks.smallbank.results;

import com.usc.dblab.cafe.QueryResult;

public class CheckingResult extends QueryResult {
	private final double bal;

	public CheckingResult(String query, double bal) {
		super(query);
		this.bal = bal;
	}	
	
	public double getBal() {
		return bal;
	}
}
