package com.oltpbenchmark.benchmarks.tpcc.procedures.results;

import com.usc.dblab.cafe.QueryResult;

public class QueryGetDist2Result extends QueryResult {

    private final String d_street_1;
    private final String d_street_2;
    private final String d_city;
    private final String d_state;
    private final String d_zip;
    private final String d_name;

    public QueryGetDist2Result(String query, String d_street_1, String d_street_2, 
            String d_city, String d_state, String d_zip, String d_name) {
        super(query);
        this.d_street_1 = d_street_1;
        this.d_street_2 = d_street_2;
        this.d_city = d_city;
        this.d_state = d_state;
        this.d_zip = d_zip;
        this.d_name = d_name;
    }

    public String getStreet1() {
        return d_street_1;
    }

    public String getStreet2() {
        return d_street_2;
    }

    public String getCity() {
        return d_city;
    }

    public String getState() {
        return d_state;
    }

    public String getZip() {
        return d_zip;
    }

    public String getName() {
        return d_name;
    }

}
