package com.oltpbenchmark.benchmarks.smallbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.usc.dblab.cafe.NgCache;

public class GetAccount extends Procedure {   
    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
            " WHERE custid = ?"
        );
    
    public void run(Connection conn, long custId, 
            NgCache cafe, Map<String, Object> tres) throws SQLException {
        try {
            cafe.startSession("GetAccount");
            
            // Get send account
            String getSendAcct = String.format(SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID, custId);
            cafe.readStatement(getSendAcct);
            
            conn.commit();
            try {
                cafe.commitSession();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (Exception e) {
            conn.rollback();
            try {
                cafe.abortSession();
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            throw new UserAbortException("Some error happens. "+ e.getMessage());
        }       
    }

    public void run(Connection conn, int custId, Map<String, Object> tres) throws SQLException {
        // Get Account Information
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custId);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + custId + "'";
            throw new UserAbortException(msg);
        }
        r0.close();
    }
}