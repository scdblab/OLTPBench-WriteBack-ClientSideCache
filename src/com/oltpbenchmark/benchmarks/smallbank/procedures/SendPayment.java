/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.smallbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.smallbank.results.CheckingResult;
import com.usc.dblab.cafe.NgCache;

/**
 * SendPayment Procedure
 * @author pavlo
 */
public class SendPayment extends Procedure {
    
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetCheckingBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ? FOR UPDATE"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal + ? " +
        " WHERE custid = ?"
    );
    
    public void run(Connection conn, long sendAcct, long destAcct, double amount, Map<String, Object> tres) throws SQLException {
        // Get Account Information
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, sendAcct);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + sendAcct + "'";
            throw new UserAbortException(msg);
        }
        
        PreparedStatement stmt1 = this.getPreparedStatement(conn, GetAccount, destAcct);
        ResultSet r1 = stmt1.executeQuery();
        if (r1.next() == false) {
            String msg = "Invalid account '" + destAcct + "'";
            throw new UserAbortException(msg);
        }
        
        // Get the sender's account balance
        PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetCheckingBalance, sendAcct);
        ResultSet balRes0 = balStmt0.executeQuery();
        if (balRes0.next() == false) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, 
                                       sendAcct);
            throw new UserAbortException(msg);
        }
        double balance = balRes0.getDouble(1);
        
        // Make sure that they have enough money
        if (balance < amount) {
            String msg = String.format("Insufficient %s funds for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, sendAcct);
            throw new UserAbortException(msg);
        }
        
        // Debt
        PreparedStatement updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount*-1d, sendAcct);
        int status = updateStmt.executeUpdate();
        assert(status == 1) :
            String.format("Failed to update %s for customer #%d [amount=%.2f]",
                          SmallBankConstants.TABLENAME_CHECKING, sendAcct, amount);
        
        // Credit
        updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, destAcct);
        status = updateStmt.executeUpdate();
        assert(status == 1) :
            String.format("Failed to update %s for customer #%d [amount=%.2f]",
                          SmallBankConstants.TABLENAME_CHECKING, destAcct, amount);
        
        generateLog(tres, sendAcct, destAcct, balance, amount);
        
        return;
    }

    public void run(Connection conn, long sendAcct, long destAcct,
    		double amount, NgCache cafe, Map<String, Object> tres) throws SQLException {
    	try {
			cafe.startSession("SendPayment");
			
			// Get send account
			String getSendAcct = String.format(SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID, sendAcct);
			cafe.readStatement(getSendAcct);
			
			// Get dest account
			String getDestAcct = String.format(SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID, destAcct);
			cafe.readStatement(getDestAcct);	
			
			// Get send's checking balance
			String getSendCheckingBalance = String.format(SmallBankConstants.QUERY_CHECKING_BAL, sendAcct);
			CheckingResult sendCheckingRes = (CheckingResult) cafe.readStatement(getSendCheckingBalance);
			if (sendCheckingRes == null) {
	            String msg = String.format("No %s for customer #%d",
	                    SmallBankConstants.TABLENAME_CHECKING, 
	                    sendAcct);
	            throw new UserAbortException(msg);
			}
			double balance = sendCheckingRes.getBal();
			
	        // Make sure that they have enough money
	        if (balance < amount) {
	            String msg = String.format("Insufficient %s funds for customer #%d",
	                                       SmallBankConstants.TABLENAME_CHECKING, sendAcct);
	            throw new UserAbortException(msg);
	        }
	        
	        // Debt
	        String updateSendCheckingBalance = String.format(SmallBankConstants.UPDATE_DECR_CHECKING_BAL, sendAcct, amount);
	        boolean success = cafe.writeStatement(updateSendCheckingBalance);
	        assert(success) :
	            String.format("Failed to update %s for customer #%d [amount=%.2f]",
	                          SmallBankConstants.TABLENAME_CHECKING, sendAcct, amount);
	        
	        // Credit
	        String updateDestCheckingBalance = String.format(SmallBankConstants.UPDATE_INCR_CHECKING_BAL, destAcct, amount);
	        success = cafe.writeStatement(updateDestCheckingBalance);	        
	        assert(success) :
	            String.format("Failed to update %s for customer #%d [amount=%.2f]",
	                          SmallBankConstants.TABLENAME_CHECKING, destAcct, amount);
    		
    		conn.commit();
			try {
				cafe.commitSession();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			generateLog(tres, sendAcct, destAcct, sendCheckingRes.getBal(), amount);
    	} catch (Exception e) {
//    	    e.printStackTrace(System.out);
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

    private void generateLog(Map<String, Object> tres, long sendAcct,
            long destAcct, double bal, double amount) {
        if (Config.ENABLE_LOGGING && tres != null) {
            tres.put(SmallBankConstants.SENDID, sendAcct);
            tres.put(SmallBankConstants.DESTID, destAcct);
            tres.put(SmallBankConstants.CHECKING_BAL, bal);
            tres.put(SmallBankConstants.AMOUNT, amount);
            if (Config.DEBUG) {
                System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
            }
        }
    }
}