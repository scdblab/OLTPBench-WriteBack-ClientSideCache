package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetCustWhseResult extends QueryResult {
    public final float c_discount;
    public final String c_last;
    public final String c_credit;
    public final float w_tax;
    
	public QueryGetCustWhseResult(String query, float c_discount, String c_last, String c_credit, float w_tax) {
		super(query);
		this.c_discount = c_discount;
		this.c_last = c_last;
		this.c_credit = c_credit;
		this.w_tax = w_tax;
	}

	public float getDiscount() {
	    return this.c_discount;
	}

	public String getLast() {
	    return this.c_last;
	}

	public String getCredit() {
	    return this.c_credit;
	}

	public float getTax() {
	    return this.w_tax;
	}

}
