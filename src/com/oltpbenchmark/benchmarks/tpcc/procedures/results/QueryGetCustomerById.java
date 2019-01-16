package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import java.sql.Timestamp;

import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.usc.dblab.cafe.QueryResult;

public class QueryGetCustomerById extends QueryResult {
    private final Customer customer;
    
	public QueryGetCustomerById(String query, Customer c) {
		super(query);
		this.customer = c;
	}
	
	public Customer getCustomer() {
	    return customer;
	}
}
