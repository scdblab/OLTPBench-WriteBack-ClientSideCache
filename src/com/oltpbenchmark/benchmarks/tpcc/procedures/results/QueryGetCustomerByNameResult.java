package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.usc.dblab.cafe.QueryResult;

public class QueryGetCustomerByNameResult extends QueryResult {
    private final List<Integer> customerIds;
    
	public QueryGetCustomerByNameResult(String query, List<Integer> customerIds) {
		super(query);
		this.customerIds = customerIds;
	}

	public List<Integer> getCustomerIds() {
	    return customerIds;
	}
}
