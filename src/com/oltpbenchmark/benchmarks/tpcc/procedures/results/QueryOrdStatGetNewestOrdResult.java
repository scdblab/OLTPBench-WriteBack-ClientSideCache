package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import java.sql.Timestamp;

import com.usc.dblab.cafe.QueryResult;

public class QueryOrdStatGetNewestOrdResult extends QueryResult {
    private int o_id;
    private int o_carrier_id;
    private Timestamp end_date;
    
	public QueryOrdStatGetNewestOrdResult(String query, int o_id, int o_carrier_id, Timestamp end_date) {
		super(query);
		this.o_id = o_id;
		this.o_carrier_id = o_carrier_id;
		this.end_date = end_date;
	}

	public int getOrderId() {
	    return o_id;
	}

	public Timestamp getTimestamp() {
		return end_date;
	}

	public int getCarrierId() {
	    return o_carrier_id;
	}

    public void setCarrierId(int parseInt) {
        this.o_carrier_id  = parseInt;   
    }

    public void setOId(int parseInt) {
        this.o_id = parseInt;
    }

    public void setDeliveryDate(long time) {
        end_date = new Timestamp(time);
    }

}
