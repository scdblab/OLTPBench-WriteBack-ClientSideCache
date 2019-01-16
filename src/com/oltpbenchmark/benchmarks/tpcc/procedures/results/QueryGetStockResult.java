package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetStockResult extends QueryResult {
    final int s_quantity;
    final String s_data;
    final String s_dist_01;
    final String s_dist_02;
    final String s_dist_03;
    final String s_dist_04;
    final String s_dist_05;
    final String s_dist_06;
    final String s_dist_07;
    final String s_dist_08;
    final String s_dist_09;
    final String s_dist_10;

	public QueryGetStockResult(String query, int s_quantity, String s_data, String s_dist_01, String s_dist_02, String s_dist_03, 
	        String s_dist_04, String s_dist_05, String s_dist_06, String s_dist_07, String s_dist_08, 
	        String s_dist_09, String s_dist_10) {
		super(query);
	    this.s_quantity = s_quantity;
	    this.s_data = s_data;
	    this.s_dist_01 = s_dist_01;
	    this.s_dist_02 = s_dist_02;
	    this.s_dist_03 = s_dist_03;
	    this.s_dist_04 = s_dist_04;
	    this.s_dist_05 = s_dist_05;
	    this.s_dist_06 = s_dist_06;
	    this.s_dist_07 = s_dist_07;
	    this.s_dist_08 = s_dist_08;
	    this.s_dist_09 = s_dist_09;
	    this.s_dist_10 = s_dist_10;	    
	}

	public int getQuantity() {
		return s_quantity;
	}

	public String getData() {
	    return s_data;
	}

	public String getDist01() {
	    return s_dist_01;
	}

	public String getDist02() {
	    return s_dist_02;
	}

	public String getDist03() {
	    return s_dist_03;
	}

	public String getDist04() {
	    return s_dist_04;
	}

	public String getDist05() {
	    return s_dist_05;
	}

	public String getDist06() {
	    return s_dist_06;
	}

	public String getDist07() {
        return s_dist_07;
	}

	public String getDist08() {
        return s_dist_08;
	}

	public String getDist09() {
        return s_dist_09;
	}

	public String getDist10() {
        return s_dist_10;
	}
}
