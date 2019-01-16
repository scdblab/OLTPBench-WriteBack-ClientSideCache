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
import com.oltpbenchmark.benchmarks.smallbank.results.SavingsResult;
import com.usc.dblab.cafe.NgCache;

/**
 * TransactSavings Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
public class TransactSavings extends Procedure {
    
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE name = ?"
    );
    
    public final SQLStmt GetSavingsBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ? FOR UPDATE"
    );
    
    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
        "   SET bal = bal - ? " +
        " WHERE custid = ?"
    );
    
    public void run(Connection conn, String custName, double amount, Map<String, Object> tres) throws SQLException {
        // First convert the custName to the acctId
        PreparedStatement stmt = this.getPreparedStatement(conn, GetAccount, custName);
        ResultSet result = stmt.executeQuery();
        if (result.next() == false) {
            String msg = "Invalid account '" + custName + "'";
            throw new UserAbortException(msg);
        }
        long custId = result.getLong(1);
        result.close();

        // Get Balance Information
        stmt = this.getPreparedStatement(conn, GetSavingsBalance, custId);
        result = stmt.executeQuery();
        if (result.next() == false) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       custId);
            throw new UserAbortException(msg);
        }
        double balance = result.getDouble(1) - amount;
        
        // Make sure that they have enough
        if (balance < 0) {
            String msg = String.format("Negative %s balance for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       custId);
            throw new UserAbortException(msg);
        }
        
        // Then update their savings balance
        stmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount, custId);
        int status = stmt.executeUpdate();
        assert(status == 1) :
            String.format("Failed to update %s for customer #%d [balance=%.2f / amount=%.2f]",
                          SmallBankConstants.TABLENAME_CHECKING, custId, balance, amount);
        
        generateLog(tres, custId, result.getDouble(1), amount);
        result.close();
        
        return;
    }

    public void run(Connection conn, String custName, double amount, 
            NgCache cafe, Map<String, Object> tres) throws SQLException {
        try {
            cafe.startSession("TransactSavings");
            
            // First convert the custName to the acctId
            String getAccount = String.format(SmallBankConstants.QUERY_ACCOUNT, custName);
            AccountResult actRes = (AccountResult) cafe.readStatement(getAccount);
            long custId = actRes.getCustId();
            
            String getSavingBalance = String.format(SmallBankConstants.QUERY_SAVINGS_BAL, custId);
            SavingsResult savingsRes = (SavingsResult)cafe.readStatement(getSavingBalance);
            if (savingsRes == null) {
                String msg = String.format("No %s for customer #%d",
                                           SmallBankConstants.TABLENAME_SAVINGS, 
                                           custId);
                throw new UserAbortException(msg);
            }
            double balance = savingsRes.getBal() - amount;
            
            // Make sure that they have enough
            if (balance < 0) {
                String msg = String.format("Negative %s balance for customer #%d",
                                           SmallBankConstants.TABLENAME_SAVINGS, 
                                           custId);
                throw new UserAbortException(msg);
            }
            
            String updateSavingsBalance = String.format(SmallBankConstants.UPDATE_DECR_SAVINGS_BAL, custId, amount);
            boolean success = cafe.writeStatement(updateSavingsBalance);
            assert(success) :
                String.format("Failed to update %s for customer #%d [balance=%.2f / amount=%.2f]",
                              SmallBankConstants.TABLENAME_CHECKING, custId, balance, amount);
            
            conn.commit();
            
            try {
                cafe.commitSession();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            generateLog(tres, custId, savingsRes.getBal(), amount);
        
        } catch (Exception e) {
//            e.printStackTrace(System.out);
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
    

    private void generateLog(Map<String, Object> tres, long custId,
            double sBal, double amount) {
        if (Config.ENABLE_LOGGING && tres != null) {
            tres.put(SmallBankConstants.CUSTID, custId);
            tres.put(SmallBankConstants.SAVINGS_BAL, sBal);
            tres.put(SmallBankConstants.AMOUNT, amount);
            if (Config.DEBUG) {
                System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
            }
        }
    }
}