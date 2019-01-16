package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetWhseResult extends QueryResult {
    final String w_street_1;
    final String w_street_2;
    final String w_city;
    final String w_state;
    final String w_zip;
    final String w_name;
    
	public QueryGetWhseResult(String query, String w_street_1, String w_street_2, String w_city, String w_state, String w_zip, String w_name) {
		super(query);
		this.w_street_1 = w_street_1;
		this.w_street_2 = w_street_2;
		this.w_city = w_city;
		this.w_state = w_state;
		this.w_zip = w_zip;
		this.w_name = w_name;
	}

    public String getStreet1() {
        return this.w_street_1;
    }

    public String getStreet2() {
        return this.w_street_2;
    }

    public String getCity() {
        return this.w_city;
    }

    public String getState() {
        return this.w_state;
    }

    public String getZip() {
        return this.w_zip;
    }

    public String getName() {
        return this.w_name;
    }

}
