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
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.smallbank.results.CheckingResult;
import com.oltpbenchmark.benchmarks.smallbank.results.SavingsResult;
import com.usc.dblab.cafe.NgCache;

/**
 * Amalgamate Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
public class Amalgamate extends Procedure {
    
    // 2013-05-05
    // In the original version of the benchmark, this is suppose to be a look up
    // on the customer's name. We don't have fast implementation of replicated 
    // secondary indexes, so we'll just ignore that part for now.
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetSavingsBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ? FOR UPDATE"
    );
    
    public final SQLStmt GetCheckingBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ? FOR UPDATE"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = ? " +
        " WHERE custid = ?"
    );
    
    public final SQLStmt ZeroSavingsBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
        "   SET bal = 0.0 " +
        " WHERE custid = ?"
    );
    
    public void run(Connection conn, long custId0, long custId1, Map<String, Object> tres) throws SQLException {
        // Get Account Information
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custId0);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + custId0 + "'";
            throw new UserAbortException(msg);
        }
        
        PreparedStatement stmt1 = this.getPreparedStatement(conn, GetAccount, custId1);
        ResultSet r1 = stmt1.executeQuery();
        if (r1.next() == false) {
            String msg = "Invalid account '" + custId1 + "'";
            throw new UserAbortException(msg);
        }
        
        // Get Balance Information
        PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetSavingsBalance, custId0);
        ResultSet balRes0 = balStmt0.executeQuery();
        if (balRes0.next() == false) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       custId0);
            throw new UserAbortException(msg);
        }
        
        PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalance, custId1);
        ResultSet balRes1 = balStmt1.executeQuery();
        if (balRes1.next() == false) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, 
                                       custId1);
            throw new UserAbortException(msg);
        }
        double savings = balRes0.getDouble(1); balRes0.close();
        double checking = balRes1.getDouble(1); balRes1.close();
        double total = savings + checking;
        // assert(total >= 0);

        // Update Balance Information
        PreparedStatement updateStmt0 = this.getPreparedStatement(conn, ZeroSavingsBalance, custId0);
        int status = updateStmt0.executeUpdate();
        assert(status == 1);
        
        PreparedStatement updateStmt1 = this.getPreparedStatement(conn, UpdateCheckingBalance, total, custId1);
        status = updateStmt1.executeUpdate();
        assert(status == 1);
        
        generateLog(tres, custId0, custId1, savings, checking, 0d, total);
        balRes0.close();
        balRes1.close();
    }

	public void run(Connection conn, long custId0, long custId1, NgCache cafe, Map<String, Object> tres) throws SQLException {
		try {
			cafe.startSession("Amalgamate");
			
			// Get send account
			String getSendAcct = String.format(SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID, custId0);
			cafe.readStatement(getSendAcct);
			
			// Get dest account
			String getDestAcct = String.format(SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID, custId1);
			cafe.readStatement(getDestAcct);
			
			// Get saving account of custId0
			String getSavingsBalance = String.format(SmallBankConstants.QUERY_SAVINGS_BAL, custId0);
			SavingsResult sr = (SavingsResult) cafe.readStatement(getSavingsBalance);
			
			// Get checking account of custId1
			String getCheckingBalance = String.format(SmallBankConstants.QUERY_CHECKING_BAL, custId1);
			CheckingResult cr = (CheckingResult) cafe.readStatement(getCheckingBalance);
			
			double total = sr.getBal() + cr.getBal();
			
			String zeroSavingsBalance = String.format(SmallBankConstants.UPDATE_SET_SAVINGS_BAL, custId0, 0d);
			boolean success = cafe.writeStatement(zeroSavingsBalance);
			assert (success);
			
			String updateCheckingBalance = String.format(SmallBankConstants.UPDATE_SET_CHECKING_BAL, custId1, total);
			success = cafe.writeStatement(updateCheckingBalance);
			assert (success);
			
			conn.commit();
			try {
				cafe.commitSession();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			generateLog(tres, custId0, custId1, sr.getBal(), cr.getBal(), 0d, total);
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

    private void generateLog(Map<String, Object> tres, long custid0, long custid1, 
            double old_sBal, double old_cBal, double sBal, double cBal) {
        if (Config.ENABLE_LOGGING && tres != null) {
            tres.put(SmallBankConstants.SENDID, custid0);
            tres.put(SmallBankConstants.DESTID, custid1);
            tres.put(SmallBankConstants.OLD_SAVINGS_BAL, old_sBal);
            tres.put(SmallBankConstants.OLD_CHECKING_BAL, old_cBal);
            tres.put(SmallBankConstants.SAVINGS_BAL, sBal);
            tres.put(SmallBankConstants.CHECKING_BAL, cBal);
            if (Config.DEBUG) {
                System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
            }
        }
    }
}