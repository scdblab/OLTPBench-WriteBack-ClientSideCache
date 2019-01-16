package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryOrderStatGetOrderLinesResult extends QueryResult {
    private int orderLinesCount;
    private long deliveryDate;
    
	public QueryOrderStatGetOrderLinesResult(String query, int orderlinesCount, long deliveryDate) {
		super(query);
		this.orderLinesCount = orderlinesCount;
		this.deliveryDate = deliveryDate;
	}

	public int getOrderLinesCount() {
	    return orderLinesCount;
	}

	public long getDeliveryDate() {
	    return deliveryDate;
	}

    public void setDeliveryDate(long deliveryDate) {
        this.deliveryDate = deliveryDate;
    }
    
    public void setOrderLinesCount(int count) {
        this.orderLinesCount = count;
    }

}
