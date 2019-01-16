package com.oltpbenchmark.benchmarks.tpcc.procedures.ro;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;
import com.usc.dblab.cafe.NgCache;

public class GetWarehouse extends TPCCProcedure {
    public SQLStmt payGetWhseSQL = new SQLStmt("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" + " FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ?");
    public PreparedStatement payGetWhse = null;

    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID,
            int numWarehouses, int terminalDistrictLowerID,
            int terminalDistrictUpperID, Map<String, Object> tres)
            throws SQLException {
        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        
        payGetWhse.setInt(1, terminalWarehouseID);
        ResultSet rs = payGetWhse.executeQuery();
        if (!rs.next())
            throw new RuntimeException("W_ID=" + terminalWarehouseID + " not found!");
        w_street_1 = rs.getString("W_STREET_1");
        w_street_2 = rs.getString("W_STREET_2");
        w_city = rs.getString("W_CITY");
        w_state = rs.getString("W_STATE");
        w_zip = rs.getString("W_ZIP");
        w_name = rs.getString("W_NAME");
        rs.close();
        rs = null;
        return null;
    }

    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID,
            int numWarehouses, int terminalDistrictLowerID,
            int terminalDistrictUpperID, NgCache cafe, Map<String, Object> tres)
            throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }
       
}
