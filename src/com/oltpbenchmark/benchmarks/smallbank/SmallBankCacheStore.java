package com.oltpbenchmark.benchmarks.smallbank;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.smallbank.results.AccountResult;
import com.oltpbenchmark.benchmarks.smallbank.results.CheckingResult;
import com.oltpbenchmark.benchmarks.smallbank.results.SavingsResult;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.CacheEntry;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.QueryResult;

public class SmallBankCacheStore extends CacheStore {
    public static final String KEY_ACCT_NAME_PREFIX = "k_act_name";
    public static final String KEY_ACCT_NAME = KEY_ACCT_NAME_PREFIX+",%s";
    
    public static final String KEY_ACCT_ID_PREFIX = "k_act_id";
    public static final String KEY_ACCT_ID = KEY_ACCT_ID_PREFIX+",%s";

    public static final String KEY_SAVINGS_PREFIX = "k_savings";
    public static final String KEY_SAVINGS_BAL = KEY_SAVINGS_PREFIX+",%s";
    
    public static final String KEY_CHECKING_PREFIX = "k_checking";
    public static final String KEY_CHECKING_BAL = KEY_CHECKING_PREFIX+",%s";

    private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();

    private Connection conn;

    public final SQLStmt GetAccountById = new SQLStmt(
            "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
            " WHERE custid = ?"
            );

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
            " WHERE custid = ?"
            );

    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
            "   SET bal = bal + ? " +
            " WHERE custid = ?"
            );

    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
            "   SET bal = bal + ? " +
            " WHERE custid = ?"
            );

    public final SQLStmt SetCheckingBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
            "   SET bal = ? " +
            " WHERE custid = ?"
            );

    public final SQLStmt SetSavingsBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
            "   SET bal = ? " +
            " WHERE custid = ?"
            );

    public SmallBankCacheStore(Connection conn) {
        this.conn = conn;
    }

    @Override
    public CacheEntry applyDelta(Delta delta, CacheEntry e) {
        if (delta.getType() != Delta.TYPE_RMW && delta.getType() != Delta.TYPE_SET) {
            throw new NotImplementedException("This function applied to a change of type RMW only.");
        }
        
        double bal = Double.parseDouble((String)e.getValue());
        double amount = Double.parseDouble((String)delta.getValue());
        switch (delta.getType()) {
            case Delta.TYPE_RMW:
                bal = bal + amount;
                break;
            case Delta.TYPE_SET:
                bal = amount;
                break;
        }
        return new CacheEntry(e.getKey(), String.valueOf(bal), true);
    }

    @Override
    public Set<CacheEntry> computeCacheEntries(String query, QueryResult result) {
        Set<CacheEntry> set = new HashSet<>();
        Set<String> keys = getReferencedKeysFromQuery(query);
        CacheEntry e = null;
        for (String key: keys) {
            String keyPrefix = key.split(",")[0];
            switch (keyPrefix) {
            case KEY_ACCT_ID_PREFIX:
                AccountResult acctRes = (AccountResult)result;
                e = new CacheEntry(key, acctRes.getName(), true);
                break;
            case KEY_ACCT_NAME_PREFIX:
                AccountResult acctResName = (AccountResult)result;
                e = new CacheEntry(key, acctResName.getCustId(), true);
                break;
            case KEY_CHECKING_PREFIX:
                CheckingResult cRes = (CheckingResult)result;
                e = new CacheEntry(key, cRes.getBal(), true);
                break;
            case KEY_SAVINGS_PREFIX:
                SavingsResult sRes = (SavingsResult)result;
                e = new CacheEntry(key, sRes.getBal(), true);
                break;
            }
        }

        if (e != null)
            set.add(e);

        return set;
    }

    @Override
    public boolean dmlDataStore(String dml) throws Exception {
        String[] tokens = dml.split(",");
        String op = tokens[0];
        long custId = Long.parseLong(tokens[1]);
        double amount = Double.parseDouble(tokens[3]);

        PreparedStatement stmt = null;
        int status = 0;
        switch (op) {
        case SmallBankConstants.UPDATE_CHECKING_PREFIX:
            switch (tokens[2]) {
            case "incr":
                stmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, custId);
                status = stmt.executeUpdate();
            case "decr":
                stmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount*-1d, custId);
                status = stmt.executeUpdate();
            case "set":
                stmt = this.getPreparedStatement(conn, SetCheckingBalance, amount, custId);
                status = stmt.executeUpdate();
            }
            break;
        case SmallBankConstants.UPDATE_SAVINGS_PREFIX:
            switch (tokens[2]) {
            case "incr":
                stmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount, custId);
                status = stmt.executeUpdate();
            case "decr":
                stmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount*-1d, custId);
                status = stmt.executeUpdate();
            case "set":
                stmt = this.getPreparedStatement(conn, SetSavingsBalance, amount, custId);
                status = stmt.executeUpdate();
                break;
            }
            break;
        }

        return status == 1 ? true: false;
    }

    @Override
    public int getHashCode(String key) {
        return Integer.parseInt(key.split(",")[1]);
    }

    @Override
    public Set<String> getImpactedKeysFromDml(String dml) {
        String[] tokens = dml.split(",");
        String op = tokens[0];
        Set<String> set = new HashSet<>();

        switch (op) {
        case SmallBankConstants.UPDATE_CHECKING_PREFIX:
            set.add(String.format(KEY_CHECKING_BAL, tokens[1]));
            break;
        case SmallBankConstants.UPDATE_SAVINGS_PREFIX:
            set.add(String.format(KEY_SAVINGS_BAL, tokens[1]));
            break;
        }

        return set;
    }

    @Override
    public Set<String> getReferencedKeysFromQuery(String query) {
        String[] tokens = query.split(",");
        String op = tokens[0];
        Set<String> set = new HashSet<>();

        switch (op) {
        case SmallBankConstants.QUERY_ACCOUNT_PREFIX:
            set.add(String.format(KEY_ACCT_NAME, tokens[1]));
            break;
        case SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID_PREFIX:
            set.add(String.format(KEY_ACCT_ID, tokens[1]));
            break;
        case SmallBankConstants.QUERY_SAVINGS_PREFIX:
            set.add(String.format(KEY_SAVINGS_BAL, tokens[1]));
            break;
        case SmallBankConstants.QUERY_CHECKING_PREFIX:
            set.add(String.format(KEY_CHECKING_BAL, tokens[1]));
            break;
        }

        return set;
    }

    @Override
    public QueryResult queryDataStore(String query) throws Exception {
        String[] tokens = query.split(",");
        String op = tokens[0];

        PreparedStatement stmt = null;
        ResultSet r0 = null;
        switch (op) {
        case SmallBankConstants.QUERY_ACCOUNT_PREFIX:
            stmt = this.getPreparedStatement(conn, GetAccount, tokens[1]);
            r0 = stmt.executeQuery();
            if (r0.next() == false) {
                String msg = "Invalid account '" + tokens[1] + "'";
                throw new UserAbortException(msg);
            }
            long custId = r0.getLong(1);
            return new AccountResult(query, custId, tokens[1]);
        case SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID_PREFIX:
            custId = Long.parseLong(tokens[1]);
            stmt = this.getPreparedStatement(conn, GetAccountById, custId);
            r0 = stmt.executeQuery();
            if (r0.next() == false) {
                String msg = "Invalid account '" + custId + "'";
                throw new UserAbortException(msg);
            }
            return new AccountResult(query, custId, r0.getString(1));
        case SmallBankConstants.QUERY_SAVINGS_PREFIX:
            custId = Long.parseLong(tokens[1]);
            stmt = this.getPreparedStatement(conn, GetSavingsBalance, custId);
            r0 = stmt.executeQuery();
            if (r0.next() == false) {
                String msg = String.format("No %s for customer #%d",
                        SmallBankConstants.TABLENAME_SAVINGS, 
                        custId);
                throw new UserAbortException(msg);
            }
            return new SavingsResult(query, r0.getDouble(1));
        case SmallBankConstants.QUERY_CHECKING_PREFIX:
            custId = Long.parseLong(tokens[1]);
            stmt = this.getPreparedStatement(conn, GetCheckingBalance, custId);
            r0 = stmt.executeQuery();
            if (r0.next() == false) {
                String msg = String.format("No %s for customer #%d",
                        SmallBankConstants.TABLENAME_CHECKING, 
                        custId);
                throw new UserAbortException(msg);
            }
            return new CheckingResult(query, r0.getDouble(1));
        }

        return null;
    }

    @Override
    public byte[] serialize(CacheEntry arg0) {
        throw new NotImplementedException("Cache entries are stored as string.");
    }

    @Override
    public byte[] serialize(Delta delta) {
        if (delta.getType() != Delta.TYPE_APPEND && delta.getType() != Delta.TYPE_SET) {
            throw new NotImplementedException("Only change of type APPEND should be serialized.");
        }
        return ((String)delta.getValue()).getBytes();
    }

    @Override
    public CacheEntry deserialize(String key, Object obj, byte[] buffer) {
        if (obj instanceof String) {
            return new CacheEntry(key, obj, true);
        } else if (obj instanceof byte[] ){
            return new CacheEntry(key, new String((byte[]) obj), true);
        }
        return null;
    }

    @Override
    public Map<String, Delta> updateCacheEntries(String dml, Set<String> keys) {
        String[] tokens = dml.split(",");
        String op = tokens[2];
        String key = keys.toArray(new String[0])[0];

        Map<String, Delta> map = new HashMap<>();
        String val = tokens[3];
        switch (op) {
        case "incr":
            map.put(key, new Delta(Delta.TYPE_RMW, val));
            break;
        case "decr":
            map.put(key, new Delta(Delta.TYPE_RMW, "-"+val));
            break;
        case "set":
            map.put(key, new Delta(Delta.TYPE_SET, val));	        
            break;
        }
        return map;
    }

    @Override
    public QueryResult computeQueryResult(String query, Set<CacheEntry> entries) {
        String op = query.split(",")[0];
        assert(entries.size() == 1);
        CacheEntry e = entries.iterator().next();
        String val = (String)e.getValue();
        
        switch (op) {
        case SmallBankConstants.QUERY_ACCOUNT_PREFIX:
            String name = query.split(",")[1];
            long custId = Long.parseLong(val);
            return new AccountResult(query, custId, name);
        case SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID_PREFIX:
            custId = Long.parseLong(query.split(",")[1]);            
            return new AccountResult(query, custId, val);
        case SmallBankConstants.QUERY_SAVINGS_PREFIX:
            double bal = Double.parseDouble(val);
            return new SavingsResult(query, bal);
        case SmallBankConstants.QUERY_CHECKING_PREFIX:
            bal = Double.parseDouble(val);
            return new CheckingResult(query, bal);
        }
        return null;
    }

    public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt, Object...params) throws SQLException {
        PreparedStatement pStmt = this.getPreparedStatementReturnKeys(conn, stmt, null);
        for (int i = 0; i < params.length; i++) {
            pStmt.setObject(i+1, params[i]);
        } // FOR
        return (pStmt);
    }

    public final PreparedStatement getPreparedStatementReturnKeys(Connection conn, SQLStmt stmt, int[] is) throws SQLException {
        assert(this.name_stmt_xref != null) : "The Procedure " + this + " has not been initialized yet!";
        PreparedStatement pStmt = this.prepardStatements.get(stmt);
        if (pStmt == null) {
            assert(this.stmt_name_xref.containsKey(stmt)) :
                "Unexpected SQLStmt handle in " + this.getClass().getSimpleName() + "\n" + this.name_stmt_xref;

            // HACK: If the target system is Postgres, wrap the PreparedStatement in a special
            //       one that fakes the getGeneratedKeys().
            if (is != null && this.dbType == DatabaseType.POSTGRES) {
                pStmt = new AutoIncrementPreparedStatement(this.dbType, conn.prepareStatement(stmt.getSQL()));
            }
            // Everyone else can use the regular getGeneratedKeys() method
            else if (is != null) {
                pStmt = conn.prepareStatement(stmt.getSQL(), is);
            }
            // They don't care about keys
            else {
                pStmt = conn.prepareStatement(stmt.getSQL());
            }
            this.prepardStatements.put(stmt, pStmt);
        }
        assert(pStmt != null) : "Unexpected null PreparedStatement for " + stmt;
        return (pStmt);
    }
}
