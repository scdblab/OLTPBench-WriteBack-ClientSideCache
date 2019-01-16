package com.oltpbenchmark.benchmarks.tpcc;

import java.util.Map;

public class TPCCState {
    int numberOfNewOrderRows;
    int numberOfHistoryRows;
    int numberOfOrderRows;
    int numberOfOrderLineRows;
    Map<Integer, Double> w_ytd_map;
    Map<Integer, Integer> d_next_o_id_map;
    Map<Integer, Double> d_ytd_map;
    
    @Override
    public boolean equals(Object otherObject) {
        if (!(otherObject instanceof TPCCState)) return false;
        TPCCState other = (TPCCState) otherObject;
        
        boolean e = true;
        
        if (numberOfHistoryRows != other.numberOfHistoryRows) {
            System.out.println("Number of History rows not match, this="+numberOfHistoryRows+
                    ", other="+other.numberOfHistoryRows);
            e = false;
        }
        
        if (numberOfNewOrderRows != other.numberOfNewOrderRows) {
            System.out.println("Number of NewOrder rows not match, this="+numberOfNewOrderRows+
                    ", other="+other.numberOfNewOrderRows);
            e = false;
        }
        
        if (numberOfOrderLineRows != other.numberOfOrderLineRows) {
            System.out.println("Number of OrderLine rows not match, this="+numberOfOrderLineRows+
                    ", other="+other.numberOfOrderLineRows);
            e = false;
        }
        
        if (numberOfOrderRows != other.numberOfOrderRows) {
            System.out.println("Number of Order rows not match, this="+numberOfOrderRows+
                    ", other="+other.numberOfOrderRows);
            e = false;
        }
        
        for (Integer w_id: w_ytd_map.keySet()) {
            double val_this = w_ytd_map.get(w_id);
            double val_other = other.w_ytd_map.get(w_id);
            if (val_this != val_other) {
                System.out.println("WAREHOUSE not match, w_id="+w_id+", this="+val_this+", other="+val_other);
                e = false;
            }
        }
        
        for (Integer d_id: d_next_o_id_map.keySet()) {
            int val_this = d_next_o_id_map.get(d_id);
            int val_other = other.d_next_o_id_map.get(d_id);
            if (val_this != val_other) {
                System.out.println("DISTRICT not match, d_id="+d_id+", this="+val_this+", other="+val_other);
                e = false;
            }
        }
        
        for (Integer d_id: d_next_o_id_map.keySet()) {
            double val_this = d_next_o_id_map.get(d_id);
            double val_other = other.d_next_o_id_map.get(d_id);
            if (val_this != val_other) {
                System.out.println("DISTRICT not match, d_id="+d_id+", this="+val_this+", other="+val_other);
                e = false;
            }
        }
        
        return e;
    }
}
