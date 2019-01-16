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
import com.oltpbenchmark.benchmarks.smallbank.results.AccountResult;
import com.oltpbenchmark.benchmarks.smallbank.results.CheckingResult;
import com.usc.dblab.cafe.NgCache;

/**
 * WriteCheck Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
public class WriteCheck extends Procedure {
    
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE name = ?"
    );
    
    public final SQLStmt GetSavingsBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetCheckingBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ? FOR UPDATE"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal - ? " +
        " WHERE custid = ?"
    );
    
    public void run(Connection conn, String custName, double amount, Map<String, Object> tres) throws SQLException {
        // First convert the custName to the custId
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custName);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + custName + "'";
            throw new UserAbortException(msg);
        }
        long custId = r0.getLong(1);
        
//        // Then get their account balances
//        PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetSavingsBalance, custId);
//        ResultSet balRes0 = balStmt0.executeQuery();
//        if (balRes0.next() == false) {
//            String msg = String.format("No %s for customer #%d",
//                                       SmallBankConstants.TABLENAME_SAVINGS, 
//                                       custId);
//            throw new UserAbortException(msg);
//        }
        
        PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalance, custId);
        ResultSet balRes1 = balStmt1.executeQuery();
        if (balRes1.next() == false) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, 
                                       custId);
            throw new UserAbortException(msg);
        }
        double balance = balRes1.getDouble(1);
        
        PreparedStatement updateStmt = null;
        if (balance < amount) {
            updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, balance, custId);
            amount = balance;
        } else {
            updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, custId);
        }
        int status = updateStmt.executeUpdate();
        assert(status == 1) :
            String.format("Failed to update %s for customer #%d [cbal=%.2f / amount=%.2f]",
                          SmallBankConstants.TABLENAME_CHECKING, custId, balance, amount);
        
        generateLog(tres, custId, balance, amount);
        
        return;
    }

	public void run(Connection conn, String custName, double amount, NgCache cafe, Map<String, Object> tres) throws SQLException {
		try {
			cafe.startSession("WriteCheck");
			
			String getAccount = String.format(SmallBankConstants.QUERY_ACCOUNT, custName);
			AccountResult actRes = (AccountResult) cafe.readStatement(getAccount);
			long custId = actRes.getCustId();
			
//			String getSavingsBalance = String.format(Config.QUERY_SAVINGS_BAL, custId);
//			SavingsResult savingsRes = (SavingsResult) cafe.readStatement(getSavingsBalance);
//			if (savingsRes == null) {
//	            String msg = String.format("No %s for customer #%d",
//	                    SmallBankConstants.TABLENAME_SAVINGS, 
//	                    custId);
//	            throw new UserAbortException(msg);
//			}
			
			String getCheckingBalance = String.format(SmallBankConstants.QUERY_CHECKING_BAL, custId);
			CheckingResult checkingRes = (CheckingResult) cafe.readStatement(getCheckingBalance);
			if (checkingRes == null) {
	            String msg = String.format("No %s for customer #%d",
	                    SmallBankConstants.TABLENAME_CHECKING, 
	                    custId);
	            throw new UserAbortException(msg);
			}
			double balance = checkingRes.getBal();
			
			String updateCheckingBalance = null;
			if (balance < amount) {
				updateCheckingBalance = String.format(SmallBankConstants.UPDATE_DECR_CHECKING_BAL, custId, balance);
				cafe.writeStatement(updateCheckingBalance);
				amount = balance;
			} else {
				updateCheckingBalance = String.format(SmallBankConstants.UPDATE_DECR_CHECKING_BAL, custId, amount);
			}
			boolean success = cafe.writeStatement(updateCheckingBalance);
			assert (success) : String.format("Failed to update %s for customer #%d [total=%.2f / amount=%.2f]",
                    SmallBankConstants.TABLENAME_CHECKING, custId, balance, amount);
			
			conn.commit();
			try {
				cafe.commitSession();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			generateLog(tres, custId, checkingRes.getBal(), amount);
		} catch (Exception e) {
//		    e.printStackTrace(System.out);
			conn.rollback();
			try {
				cafe.abortSession();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			throw new UserAbortException("Some error happens. "+ e.getMessage());
		}
        
        return;		
	}

    private void generateLog(Map<String, Object> tres, long custId,
            double bal, double amount) {
        if (Config.ENABLE_LOGGING && tres != null) {
            tres.put(SmallBankConstants.CUSTID, custId);
            tres.put(SmallBankConstants.CHECKING_BAL, bal);
            tres.put(SmallBankConstants.AMOUNT, amount);
            if (Config.DEBUG) {
                System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
            }
        }
    }
}