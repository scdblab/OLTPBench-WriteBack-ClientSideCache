package com.oltpbenchmark.benchmarks.tpcc;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.benchmarks.smallbank.validation.DBState;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;


public class TPCCOptimization {
    private static String inputFile = "/home/hieun/Desktop/EW/oltpbench-wb/sessWriter.txt";
    private static String dbip = "10.0.0.220";
    private static String dbuser = "hieun";
    private static String dbpass = "golinux";
    private static String mysqlfolder = "/var/lib/mysql/";
    private static String copyfolder = "/home/hieun/Desktop/data/tpcc1w/";
    
    
    private static final boolean printBatch = false;
    private static final boolean execute = true;
    
    // different merging options
    private static boolean mergeInsert = true;
    private static boolean mergeDelete = true;
    private static boolean mergeUpdate = true;
    private static TPCCState initState = null;
    private static TPCCState oldState = null;
    
    public static Connection getConn() {
        String url = "jdbc:mysql://"+dbip+":3306/tpcc?serverTimezone=UTC";
        String driver = "com.mysql.jdbc.Driver";
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, dbuser, dbpass);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }       

        return conn;
    }
    
//    public static int totalDmls = 0;
//    public static int totalExecDmls = 0;
//    public static int mergeInsertDmls = 0;
//    public static int mergeUpdateDmls = 0;
//    public static int mergeDeleteDmls = 0;
    
    public static void main(String[] args) {
        dbip = args[0];
        dbuser = args[1];
        dbpass = args[2];
        mysqlfolder=args[3];
        copyfolder = args[4];
        inputFile = args[5];
        
        List<String> lines = null;
        try {
            lines = Utilities.readFromFile(inputFile);
        } catch (Exception e) {
            System.out.println("Error in reading the file");
            System.exit(-1);
        }
        
        for (int i = 0; i < lines.size(); ++i) {
            String line = lines.get(i);
            String token = "UPDATE ORDER_LINE SET OL_DELIVERY_D=";
            if (line.contains(token)) {
                long ts = Long.parseLong(line.substring(token.length()).split(" ")[0]);
                Date date = new Date(ts);
                String s = date.toString();
                line = line.replace(String.valueOf(ts), "'"+s+"'");
                lines.set(i, line);
            }
            
            if (line.contains("INSERT INTO HISTORY")) {
                String s = line.split(",")[11];
                line = line.replace(s, "'"+s+"'");
                long ts = Long.parseLong(line.split(",")[12]);
                Date date = new Date(ts);
                s = date.toString();
                line = line.replace(String.valueOf(ts), "'"+s+"'");
                lines.set(i, line);
            }
            
            if (line.contains("INSERT INTO OORDER")) {
                long ts = Long.parseLong(line.split(",")[9]);
                Date date = new Date(ts);
                String s = date.toString();
                line = line.replace(String.valueOf(ts), "'"+s+"'");
                lines.set(i, line);
            }
            
            if (line.contains("INSERT INTO ORDER_LINE")) {
                String s= line.split(",")[9];
                line = line.replace(s, "'"+s+"'");
                lines.set(i, line);
            }
            
            if (line.contains("UPDATE CUSTOMER SET C_BALANCE") && line.contains("C_DATA")) {
                String s = line.split(",")[1].split("=")[1];
                line = line.replace(s, "'"+s+"'");
                lines.set(i, line);
            }
        }
        
        int[] batches = { 1, 10, 100 };
        Stats stats = Stats.getStatsInstance(1);
        
        
        System.out.println("Batch size = 1 , optimize= false");
        runLog(1, false, lines, stats);
        
        for (int batch: batches) {
            System.out.println("Batch size = "+batch+" , optimize= true");
            runLog(batch, true, lines, stats);
        }
    }
    
    private static void runLog(int batch, boolean optimize, List<String> lines, Stats stats) {
        Connection conn = null;
        Statement stmt = null;
        if (execute) {
            Utilities.restoreMySQLLinux(dbip, mysqlfolder, copyfolder);

            try {
                conn = getConn();
                if (conn != null)
                    stmt = conn.createStatement();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace(System.out);
            } catch (Exception e) { 
                e.printStackTrace(System.out);
            }
        }
        
        initState = getTPCCState(stmt);
        
        long time = System.nanoTime();
        
        try {
            exec(batch, optimize, lines, stats, conn, stmt, null);
        } catch (SQLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        time = System.nanoTime() - time;
        if (stats != null) stats.incrBy("apply_bw_time", (time/1000/1000/1000));
        
        TPCCState currState = getTPCCState(stmt);
        if (oldState != null) {
            System.out.println("State matches? " + currState.equals(oldState));
        }
        oldState = currState;
        
        try {
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println(Stats.getAllStats().toString(4));
        stats.reset();
    }

    private static TPCCState getTPCCState(Statement stmt) {
        TPCCState state = new TPCCState();
        state.numberOfNewOrderRows = getCounter(stmt, "SELECT COUNT(*) FROM NEW_ORDER");
        state.numberOfHistoryRows = getCounter(stmt, "SELECT COUNT(*) FROM HISTORY");
        state.numberOfOrderLineRows = getCounter(stmt, "SELECT COUNT(*) FROM ORDER_LINE");
        state.numberOfOrderRows = getCounter(stmt, "SELECT COUNT(*) FROM OORDER");
        
        String query = "SELECT W_ID, W_YTD FROM WAREHOUSE";
        state.w_ytd_map = new HashMap<>();
        try {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                int w_id = rs.getInt(1);
                double w_ytd = rs.getDouble(2);
                state.w_ytd_map.put(w_id, w_ytd);
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        state.d_next_o_id_map = new HashMap<>();
        state.d_ytd_map = new HashMap<>();
        query = "SELECT D_ID, D_YTD, D_NEXT_O_ID FROM DISTRICT";
        try {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                int d_id = rs.getInt(1);
                double d_ytd = rs.getDouble(2);
                int d_next_o_id = rs.getInt(3);
                state.d_next_o_id_map.put(d_id, d_next_o_id);
                state.d_ytd_map.put(d_id, d_ytd);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return state;
    }

    static void exec(int batch, boolean optimize, List<String> lines, 
            Stats stats, Connection conn, Statement stmt, WriteBack cacheBack) throws SQLException {
        List<List<String>> transactions = new ArrayList<>();
        List<List<String>> sessList = new ArrayList<>();
        
        List<String> dmls = new ArrayList<>();
        List<String> sessIds = new ArrayList<>();
        int b = 0;
        for (String line: lines) {
            if (line.equals("")) {
                continue;
            }
            
            if (line.contains("-") && line.substring(line.indexOf("-")+1).length() == 36) {
//            if (line.length() == 36) {  // this line contains the session id
                if (b == batch) {
                    transactions.add(dmls);
                    dmls = new ArrayList<>();
                    
                    sessList.add(sessIds);                    
                    sessIds = new ArrayList<>();
                    sessIds.add(line);
                    
                    b = 0;
                } else {
                    sessIds.add(line);
                    b++;
                }
                continue;
            }
            
            if (stats != null) stats.incr("total_dmls");
            
            // convert the line to a dml object and add to dmls
            dmls.add(line);
        }
        
        if (dmls.size() > 0) {
            transactions.add(dmls);
            sessList.add(sessIds);  
        }
        
        List<List<String>> toExecute = new ArrayList<>();
        
        for (List<String> transaction: transactions) {
            List<String> ts = transaction;
            
            if (optimize) {
                ts = optimize(ts, stats);
            } else {
                ts = reOrder(ts);
            }
            
            if (printBatch) {
                System.out.println("========");
                for (String d: ts) {
                    System.out.println(d);
                }
            }
            
            toExecute.add(ts);
        }
        
        if (execute) {
            for (int i = 0; i < toExecute.size(); ++i) {
                List<String> ts = toExecute.get(i);

                for (String d: ts) {
                    stmt.execute(d);
                }
                        
                // insert the session id to the Session table
                // cacheBack.insertCommitedSessionRows(sessList.get(i));
            }
        }
    }
    
    private static List<String> reOrder(List<String> ts) {
        // just push the insertion to OORDER to be the first, before inserting to NEW_ORDER
        for (int i = 0; i < ts.size(); ++i) {
            if (ts.get(i).contains("INSERT INTO OORDER")) {
                String dml = ts.remove(i);
                ts.add(0, dml);
            }
        }
        return ts;
    }
    
    public static int getCounter(Statement stmt, String query) {
        try {
            ResultSet rs = stmt.executeQuery(query);
            int val = -1;
            if (rs.next()) {
                val = rs.getInt(1);
            }
            rs.close();
            return val;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return -1;
    }

    private static List<String> optimize(List<String> transaction, Stats stats) {
        List<String> res = new ArrayList<>();
        
        String newOrderStr = "INSERT INTO NEW_ORDER(NO_D_ID,NO_O_ID,NO_W_ID) VALUES ";
        String orderStr = "INSERT INTO OORDER(O_ALL_LOCAL,O_C_ID,O_D_ID,O_ENTRY_D,O_ID,O_OL_CNT,O_W_ID) VALUES ";
        String orderLineStr = "INSERT INTO ORDER_LINE(OL_AMOUNT,OL_DIST_INFO,OL_D_ID,OL_I_ID,OL_NUMBER,OL_O_ID,OL_QUANTITY,OL_SUPPLY_W_ID,OL_W_ID) VALUES ";
        String historyStr = "INSERT INTO HISTORY(H_AMOUNT,H_C_D_ID,H_C_ID,H_C_W_ID,H_DATA,H_DATE,H_D_ID,H_W_ID) VALUES ";
        
        String deleteStr = "DELETE FROM NEW_ORDER WHERE ";
        
        Map<Integer, Double> w_ytd = new HashMap<>();
        
        Map<Integer, Integer> d_next_o_id = new HashMap<>();
        Map<Integer, Double> d_ytd = new HashMap<>();
        int id = -1;
        
        for (String dml: transaction) {
            if (mergeInsert && dml.startsWith("INSERT INTO")) {                
                String table = dml.split(" ")[2].split("\\(")[0];
                
                if (table.equals("HISTORY") || table.equals("NEW_ORDER") ||
                        table.equals("OORDER") || table.equals("ORDER_LINE")) {
                    String values = dml.substring(dml.indexOf("VALUES ")+"VALUES ".length());
                    switch (table) {
                    case "HISTORY":
                        historyStr += values+", ";
                        break;
                    case "OORDER":
                        orderStr += values+", ";
                        break;
                    case "NEW_ORDER":
                        newOrderStr += values+", ";
                        break;
                    case "ORDER_LINE":
                        orderLineStr += values+", ";
                        break;
                    }
                    if (stats != null) stats.incr("merge_insert_dmls");
                } else {
                    res.add(dml);
                }
                continue;
            } 
            
            if (mergeUpdate && dml.startsWith("UPDATE ")) {
                String table = dml.split(" ")[1];
                
                if (table.equals("DISTRICT")) {
                    if (dml.contains("D_NEXT_O_ID")) {
                        int d_id = Integer.parseInt(dml.split("=")[2].split(" ")[0]);
                        int w_id = Integer.parseInt(dml.split("=")[3]);
                        
                        if (id == -1) id = w_id;
                        else if (w_id != id) System.out.println("Error: Different warehouse");
                        
                        Integer x = d_next_o_id.get(d_id);
                        if (x == null) {
                            d_next_o_id.put(d_id, 1);
                        } else {
                            d_next_o_id.put(d_id, x+1);
                        }
                        if (stats != null) stats.incr("merge_update_dmls");
                    }
                    
                    if (dml.contains("D_YTD")) {
                        int d_id = Integer.parseInt(dml.split("=")[2].split(" ")[0]);
                        double val = Double.parseDouble(dml.split("\\+")[1].split(" ")[0]);
                        
                        int w_id = Integer.parseInt(dml.split("=")[3]);
                        if (id == -1) id = w_id;
                        else if (w_id != id) System.out.println("Error: Different warehouse");
                        
                        Double x = d_ytd.get(d_id);
                        if (x == null) {
                            d_ytd.put(d_id, val);
                        } else {
                            d_ytd.put(d_id, x+val);
                        }
                        if (stats != null) stats.incr("merge_update_dmls");
                    }
                } else if (table.equals("WAREHOUSE")) {
                    int w_id = Integer.parseInt(dml.split("=")[2].split(" ")[0]);
                    double val = Double.parseDouble(dml.split("\\+")[1].split(" ")[0]);
                    Double x = w_ytd.get(w_id);
                    if (x == null) {
                        w_ytd.put(w_id, val);
                    } else {
                        w_ytd.put(w_id, x+val);
                    }
                    if (stats != null) stats.incr("merge_update_dmls");
                } else {
                    res.add(dml);
                }
                continue;
            }
            
            if (mergeDelete && dml.startsWith("DELETE FROM ")) {
                String table = dml.split(" ")[2];
                
                if (table.equals("NEW_ORDER")) {
                    String pred = dml.split("WHERE ")[1];
                    deleteStr += "("+pred+") OR ";
                    if (stats != null) stats.incr("merge_delete_dmls");
                } else {
                    res.add(dml);
                }
            }
            

            // finally, the dml has no change and should be added to be executed
            res.add(dml);
        }
        
        if (mergeInsert) {
            if (historyStr.endsWith(", ")) {
                historyStr = historyStr.substring(0, historyStr.length()-2);
                res.add(0, historyStr);
                if (stats != null) stats.decr("merge_insert_dmls");
            }
            
            if (orderLineStr.endsWith(", ")) {
                orderLineStr = orderLineStr.substring(0, orderLineStr.length()-2);
                res.add(0, orderLineStr);
                if (stats != null) stats.decr("merge_insert_dmls");
            }
            
            if (newOrderStr.endsWith(", ")) {
                newOrderStr = newOrderStr.substring(0, newOrderStr.length()-2);
                res.add(0, newOrderStr);
                if (stats != null) stats.decr("merge_insert_dmls");
            }
            
            if (orderStr.endsWith(", ")) {
                orderStr = orderStr.substring(0, orderStr.length()-2);
                res.add(0, orderStr);
                if (stats != null) stats.decr("merge_insert_dmls");
            }
        }
        
        if (mergeUpdate) {
            for (Integer w_id: w_ytd.keySet()) {
                double val = w_ytd.get(w_id);
                res.add("UPDATE WAREHOUSE SET W_YTD=W_YTD+"+val+" WHERE W_ID="+w_id);
                if (stats != null) stats.decr("merge_update_dmls");
            }
            
            for (Integer d_id: d_ytd.keySet()) {
                double ytd = d_ytd.get(d_id);
                Integer next_o_id = d_next_o_id.get(d_id);
                if (next_o_id != null) {
                    d_next_o_id.remove(d_id);
                    res.add("UPDATE DISTRICT SET D_NEXT_O_ID=D_NEXT_O_ID+"+next_o_id+", D_YTD=D_YTD+"+ytd+
                            " WHERE D_ID="+d_id+" AND D_W_ID="+id);
                } else {
                    res.add("UPDATE DISTRICT SET D_YTD=D_YTD+"+ytd+" WHERE D_ID="+d_id+" AND D_W_ID="+id);
                }
                if (stats != null) stats.decr("merge_update_dmls");
            }
            
            for (Integer d_id: d_next_o_id.keySet()) {
                int next_o_id = d_next_o_id.get(d_id);
                res.add("UPDATE DISTRICT SET D_NEXT_O_ID=D_NEXT_O_ID+"+next_o_id+" WHERE D_ID="+d_id+" AND D_W_ID="+id);
                if (stats != null) stats.decr("merge_update_dmls");
            }
        }
        
        if (mergeDelete) {
            if (deleteStr.endsWith("OR ")) {
                deleteStr = deleteStr.substring(0, deleteStr.length()-3);
                res.add(deleteStr);
                if (stats != null) stats.decr("merge_delete_dmls");
            }
        }
        
        return res;
    }
}
