package com.oltpbenchmark.benchmarks.smallbank;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.Config;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Session;

public class SmallBankWriteBack extends WriteBack {
    public static final String DATA_ITEM_CHECKING = SmallBankConstants.TABLENAME_CHECKING+",%s";
    public static final String DATA_ITEM_SAVINGS = SmallBankConstants.TABLENAME_SAVINGS+",%s";

    private final Connection conn;

    private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();

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

    public SmallBankWriteBack(Connection conn) {
        this.conn = conn;
    }

    @Override
    public boolean applyBufferedWrite(String arg0, Object arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public LinkedHashMap<String, Change> bufferChanges(String dml, Set<String> buffKeys) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object convertToIdempotent(Object arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Change deserialize(byte[] bytes) {
        return new Change(Change.TYPE_APPEND, new String(bytes));
    }

    @Override
    public Set<String> getMapping(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isIdempotent(Object buffValue) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Set<String> rationalizeRead(String query) {
        String[] tokens = query.split(",");
        Set<String> set = new HashSet<>();
        switch (tokens[0]) {
        case SmallBankConstants.QUERY_CHECKING_PREFIX:
            set.add(String.format(DATA_ITEM_CHECKING, tokens[1]));
            break;
        case SmallBankConstants.QUERY_SAVINGS_PREFIX:
            set.add(String.format(DATA_ITEM_SAVINGS, tokens[1]));
            break;
        }
        return set;
    }

    @Override
    public LinkedHashMap<String, Change> rationalizeWrite(String dml) {
        String[] tokens = dml.split(",");
        LinkedHashMap<String, Change> changes = new LinkedHashMap<>();

        String obj = null;
        String amount = tokens[3];

        switch (tokens[2]) {
        case "incr":
            obj = "+"+amount;
            break;
        case "decr":
            obj = "-"+amount;
            break;
        case "set":
            obj = "="+amount;
            break;
        }

        if (tokens[0].equals(SmallBankConstants.UPDATE_CHECKING_PREFIX)) {
            changes.put(String.format(DATA_ITEM_CHECKING, tokens[1]), new Change(Change.TYPE_APPEND, obj));
        } else if (tokens[0].equals(SmallBankConstants.UPDATE_SAVINGS_PREFIX)) {
            changes.put(String.format(DATA_ITEM_SAVINGS, tokens[1]), new Change(Change.TYPE_APPEND, obj));
        }

        return changes;
    }

    @Override
    public byte[] serialize(Change change) {
        if (change.getType() != Change.TYPE_APPEND) {
            throw new NotImplementedException("Should an append.");
        }
        return ((String)change.getValue()).getBytes();
    }

    @Override
    public boolean applySessions(List<Session> sessions, Connection conn, 
            Statement stmt, PrintWriter sessionWriter, Stats stats) throws Exception {
        Map<String, String> mergeMap = new HashMap<>();

        for (Session sess: sessions) {
            List<String> its = sess.getIdentifiers();
            for (int i = 0; i < its.size(); ++i) {
                String identifier = its.get(i);
                Change change = sess.getChange(i);
                String val = mergeMap.get(identifier);
                if (val == null) {
                    mergeMap.put(identifier, (String)change.getValue());
                } else {
                    String cval = (String)change.getValue();
                    char c_op = cval.charAt(0);
                    double c_x = Double.parseDouble(cval.substring(1));
                    char op = val.charAt(0);
                    double x = Double.parseDouble(val.substring(1));
                    switch (c_op) {
                    case '=':
                        val = cval;
                        break;
                    case '-': c_x = -c_x;
                    case '+':
                        if (op == '-') x = -x;
                        x = x + c_x;
                        
                        if (op == '=') val = String.format("%c%.2f", op, x);
                        else if (x >= 0) val = String.format("+%.2f", x);
                        else val = String.format("-%.2f", x);
                        break;
                    }
                    
                    mergeMap.put(identifier, val);
                }
            }
        }

        PreparedStatement prepStmt = null;
        for (String it: mergeMap.keySet()) {
            String val = mergeMap.get(it);
            char op = val.charAt(0);
            double amount = Double.parseDouble(val.substring(1));

            String[] tokens = it.split(",");
            String table = tokens[0];
            long custId = Long.parseLong(tokens[1]);
            switch (table) {
            case SmallBankConstants.TABLENAME_CHECKING:
                switch (op) {
                case '+':
                    prepStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, custId);
                    break;
                case '-':
                    prepStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount*-1d, custId);
                    break;
                case '=':
                    prepStmt = this.getPreparedStatement(conn, SetCheckingBalance, amount, custId);
                    break;
                }
                break;
            case SmallBankConstants.TABLENAME_SAVINGS:
                switch (op) {
                case '+':
                    prepStmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount, custId);
                    break;
                case '-':
                    prepStmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount*-1d, custId);
                    break;
                case '=':
                    prepStmt = this.getPreparedStatement(conn, SetSavingsBalance, amount, custId);
                    break;
                }
                break;
            }

            if (stmt != null) {
                int status = prepStmt.executeUpdate();
                assert(status == 1);
            }
        }
        //conn.commit();

        return true;
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

    @Override
    public QueryResult merge(String query, QueryResult result,
            LinkedHashMap<String, List<Change>> buffVals) {
        return result;
    }

    @Override
    public byte[] serializeSessionChanges(Map<String, List<Change>> changesMap) {
        int totalSize = 0;

        LinkedHashMap<String, byte[][]> bytesMap = new LinkedHashMap<>();
        for (String it: changesMap.keySet()) {
            totalSize += 4+it.length();

            List<Change> changes = changesMap.get(it);
            totalSize += 4; // storing the size of the list

            byte[][] bytesList = new byte[changes.size()][];
            for (int i = 0; i < changes.size(); ++i) {
                Change c = changes.get(i);
                byte[] bytes = serialize(c);
                bytesList[i] = bytes;
                
                totalSize += 4;
                totalSize += bytes.length;
            }

            bytesMap.put(it, bytesList);
        }

        ByteBuffer buff = ByteBuffer.allocate(totalSize);
        for (String it: changesMap.keySet()) {
            buff.putInt(it.length());
            buff.put(it.getBytes());

            byte[][] bytesList = bytesMap.get(it);
            buff.putInt(bytesList.length);
            for (int i = 0; i < bytesList.length; ++i) {
                buff.putInt(bytesList[i].length);
                buff.put(bytesList[i]);
            }
        }

        return buff.array();
    }

    @Override
    public int getTeleWPartition(String sessId) {
        int hc = sessId.hashCode();
        if (hc < 0) {
            hc = -hc;
        }
        return hc % Config.NUM_PENDING_WRITES_LOGS;
    }

    @Override
    public Map<String, List<Change>> deserializeSessionChanges(byte[] bytes) {
        ByteBuffer buff = ByteBuffer.wrap(bytes);

        int offset = 0;
        LinkedHashMap<String, List<Change>> changesMap = new LinkedHashMap<>();
        while (offset < bytes.length) {
            int sz = buff.getInt();
            byte[] b = new byte[sz];
            buff.get(b);
            String it = new String(b);
            offset += 4+sz;

            sz = buff.getInt();
            offset += 4;

            List<Change> changes = new ArrayList<>();
            for (int i = 0; i < sz; ++i) {
                int len = buff.getInt();
                offset += 4;

                b = new byte[len];
                buff.get(b);
                offset += len;
                Change c = deserialize(b);
                changes.add(c);
            }

            changesMap.put(it, changes);
        }

        return changesMap;
    }
}
