package com.oltpbenchmark.benchmarks.tpcc;

import java.util.Map;
import java.util.TreeMap;

public class DML {
    public static final int INSERT = 0;
    public static final int UPDATE = 1;
    public static final int DELETE = 2;
    
    private final String table;
    int type;
    
    private final TreeMap<String, String> updateClause;
    private final TreeMap<String, String> whereClause;
    
    public DML(String table, int type, 
            TreeMap<String, String> updateClause, TreeMap<String, String> whereClause) {
        this.table = table;
        this.type = type;
        this.updateClause = updateClause;
        this.whereClause = whereClause;
    }

    public String getTable() {
        return table;
    }

    public int getType() {
        return type;
    }

    public Map<String, String> getUpdateClause() {
        return updateClause;
    }

    public Map<String, String> getWhereClause() {
        return whereClause;
    }
    
    public String toSQL() {
        StringBuilder sb = new StringBuilder();
        
        switch (type) {
        case DML.INSERT:
            sb.append("INSERT INTO "+table+"(");
            for (String key: updateClause.keySet()) {
                sb.append(key+",");
            }
            sb.setLength(sb.length()-1);
            sb.append(") VALUES (");
            for (String key: updateClause.keySet()) {
                sb.append(updateClause.get(key)+",");
            }
            sb.setLength(sb.length()-1);
            sb.append(")");
            break;
        case DML.UPDATE:
            sb.append("UPDATE "+table+" SET ");
            for (String key: updateClause.keySet()) {
                sb.append(key+"="+updateClause.get(key)+",");
            }
            sb.setLength(sb.length()-1);
            sb.append(" WHERE ");
            for (String key: whereClause.keySet()) {
                sb.append(key+"="+whereClause.get(key)+" AND ");
            }
            sb.setLength(sb.length()-" AND ".length());
            break;
        case DML.DELETE:
            sb.append("DELETE FROM "+table+" WHERE ");
            for (String key: whereClause.keySet()) {
                sb.append(key+"="+whereClause.get(key)+" AND ");
            }
            sb.setLength(sb.length()-" AND ".length());
            break;
        }
        
        return sb.toString();
    }
    
    public String toPreparedSQL() {
        StringBuilder sb = new StringBuilder();
        
        switch (type) {
        case DML.INSERT:
            sb.append("INSERT INTO "+table+"(");
            for (String key: updateClause.keySet()) {
                sb.append(key+",");
            }
            sb.setLength(sb.length()-1);
            sb.append(") VALUES (");
            for (String key: updateClause.keySet()) {
                String val = updateClause.get(key);
                if (val.contains(key+"+"))
                    sb.append(key+"+?,");
                else if (val.contains(key+"-"))
                    sb.append(key+"-?,");
                else
                    sb.append("?,");
            }
            sb.setLength(sb.length()-1);
            sb.append(")");
            break;
        case DML.UPDATE:
            sb.append("UPDATE "+table+" SET ");
            for (String key: updateClause.keySet()) {
                sb.append(key+"=");
                String val = updateClause.get(key);
                if (val.contains(key+"+"))
                    sb.append(key+"+?,");
                else if (val.contains(key+"-"))
                    sb.append(key+"-?,");
                else
                    sb.append("?,");                
            }
            sb.setLength(sb.length()-1);
            sb.append(" WHERE ");
            for (String key: whereClause.keySet()) {
                sb.append(key+"=? AND ");
            }
            sb.setLength(sb.length()-" AND ".length());
            break;
        case DML.DELETE:
            sb.append("DELETE FROM "+table+" WHERE ");
            for (String key: whereClause.keySet()) {
                sb.append(key+"=? AND ");
            }
            sb.setLength(sb.length()-" AND ".length());
            break;
        }
        
        return sb.toString();
    }
}
