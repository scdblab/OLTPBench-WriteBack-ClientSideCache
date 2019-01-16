package com.oltpbenchmark.benchmarks.smallbank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Amalgamate;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Balance;
import com.oltpbenchmark.benchmarks.smallbank.procedures.DepositChecking;
import com.oltpbenchmark.benchmarks.smallbank.procedures.SendPayment;
import com.oltpbenchmark.benchmarks.smallbank.procedures.TransactSavings;
import com.oltpbenchmark.benchmarks.smallbank.procedures.WriteCheck;
import com.oltpbenchmark.benchmarks.smallbank.validation.Entity;
import com.oltpbenchmark.benchmarks.smallbank.validation.Property;
import com.oltpbenchmark.benchmarks.smallbank.validation.Utilities;
import com.oltpbenchmark.benchmarks.smallbank.validation.ValidationConstants;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomDistribution.*;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.Stats;
import com.oltpbenchmark.benchmarks.Config;

/**
 * SmallBank Benchmark Work Driver
 * Fuck yo couch.
 * @author pavlo
 * 
 */
public class SmallBankWorker extends Worker<SmallBankBenchmark> {
    private static final Logger LOG = Logger.getLogger(SmallBankWorker.class);

    private final Amalgamate procAmalgamate;
    private final Balance procBalance;
    private final DepositChecking procDepositChecking;
    private final SendPayment procSendPayment;
    private final TransactSavings procTransactSavings;
    private final WriteCheck procWriteCheck;

    private final DiscreteRNG rng;
    private final long numAccounts;
    private final int custNameLength;
    private final String custNameFormat;
    private final long custIdsBuffer[] = { -1l, -1l };

    public NgCache cafe = null;
    private CacheStore cacheStore;
    private WriteBack cacheBack;
    
    private int threadId;
    private int sequenceId = 0;
    
    public HashMap<String, Object> transactionResults = null;
    public BufferedWriter updateLogAll = null;
    public BufferedWriter readLogAll = null;
    private StringBuilder readLog = null;
    private StringBuilder updateLog = null;

    public SmallBankWorker(SmallBankBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);

        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on 
        // a client machine with not a lot of cores
        this.procAmalgamate = this.getProcedure(Amalgamate.class);
        this.procBalance = this.getProcedure(Balance.class);
        this.procDepositChecking = this.getProcedure(DepositChecking.class);
        this.procSendPayment = this.getProcedure(SendPayment.class);
        this.procTransactSavings = this.getProcedure(TransactSavings.class);
        this.procWriteCheck = this.getProcedure(WriteCheck.class);

        this.numAccounts = benchmarkModule.numAccounts;
        this.custNameLength = SmallBankBenchmark.getCustomerNameLength(benchmarkModule.getTableCatalog(SmallBankConstants.TABLENAME_ACCOUNTS));
        this.custNameFormat = "%0"+this.custNameLength+"d";
        this.rng = new Flat(rng(), 0, this.numAccounts);
        
        this.threadId = id;

        if (Config.CAFE) {
            cacheStore = new SmallBankCacheStore(conn);
            cacheBack = new SmallBankWriteBack(conn);
            Stats stats = Stats.getStatsInstance(threadId);
            if (SmallBankConstants.STATS)
                Stats.stats = true;
            this.cafe = new NgCache(cacheStore, cacheBack, Config.CACHE_POOL_NAME, CachePolicy.WRITE_BACK, 0, stats,
                    this.benchmarkModule.workConf.getDBConnection(), this.benchmarkModule.workConf.getDBName(), 
                    this.benchmarkModule.workConf.getDBPassword(), true, Config.AR_SLEEP, 0, 10);
        }

        if (Config.ENABLE_LOGGING) {
            try {
                String dir = SmallBankConstants.TRACE_LOGGING_DIR;
                File ufile = new File(dir + "/update0-" + threadId + ".txt");
                FileWriter ufstream = new FileWriter(ufile);
                updateLogAll = new BufferedWriter(ufstream);
                // read file
                File rfile = new File(dir + "/read0-" + threadId + ".txt");
                FileWriter rfstream = new FileWriter(rfile);
                readLogAll = new BufferedWriter(rfstream);
                readLog = new StringBuilder();
                updateLog = new StringBuilder();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            transactionResults = new HashMap<String, Object>();
        }
    }

    protected void generateCustIds(boolean needsTwoAccts) {
        for (int i = 0; i < this.custIdsBuffer.length; i++) {
            this.custIdsBuffer[i] = this.rng.nextLong();

            // They can never be the same!
            if (i > 0 && this.custIdsBuffer[i-1] == this.custIdsBuffer[i]) {
                i--;
                continue;
            }

            // If we only need one acctId, break out here.
            if (i == 0 && needsTwoAccts == false) break;
            // If we need two acctIds, then we need to go generate the second one
            if (i == 0) continue;

        } // FOR
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Accounts: %s", Arrays.toString(this.custIdsBuffer)));
    }


    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = txnType.getProcedureClass();
        long startTime = System.nanoTime();
        String transName = procClass.getSimpleName();
        //System.out.println(transName);

        // Amalgamate
        if (procClass.equals(Amalgamate.class)) {
            this.generateCustIds(true);
            if (Config.CAFE) {
                this.procAmalgamate.run(conn, this.custIdsBuffer[0], this.custIdsBuffer[1], this.cafe, this.transactionResults);
            } else {
                this.procAmalgamate.run(conn, this.custIdsBuffer[0], this.custIdsBuffer[1], this.transactionResults);
            }

            // Balance
        } else if (procClass.equals(Balance.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            if (Config.CAFE) {
                this.procBalance.run(conn, custName, this.cafe, this.transactionResults);
            } else {
                this.procBalance.run(conn, custName, this.transactionResults);
            }

            // DepositChecking
        } else if (procClass.equals(DepositChecking.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            if (Config.CAFE) {
                this.procDepositChecking.run(conn, custName, SmallBankConstants.PARAM_DEPOSIT_CHECKING_AMOUNT, this.cafe, this.transactionResults);
            } else {
                this.procDepositChecking.run(conn, custName, SmallBankConstants.PARAM_DEPOSIT_CHECKING_AMOUNT, this.transactionResults);
            }

            // SendPayment
        } else if (procClass.equals(SendPayment.class)) {
            this.generateCustIds(true);
            if (Config.CAFE) {
                this.procSendPayment.run(conn, this.custIdsBuffer[0], 
                        this.custIdsBuffer[1], SmallBankConstants.PARAM_SEND_PAYMENT_AMOUNT, this.cafe, this.transactionResults);
            } else {
                this.procSendPayment.run(conn, this.custIdsBuffer[0], 
                        this.custIdsBuffer[1], SmallBankConstants.PARAM_SEND_PAYMENT_AMOUNT, this.transactionResults);
            }

            // TransactSavings
        } else if (procClass.equals(TransactSavings.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            if (Config.CAFE) {
                this.procTransactSavings.run(conn, custName, SmallBankConstants.PARAM_TRANSACT_SAVINGS_AMOUNT, this.cafe, this.transactionResults);
            } else {
                this.procTransactSavings.run(conn, custName, SmallBankConstants.PARAM_TRANSACT_SAVINGS_AMOUNT, this.transactionResults);
            }

            // WriteCheck
        } else if (procClass.equals(WriteCheck.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            if (Config.CAFE) {
                this.procWriteCheck.run(conn, custName, SmallBankConstants.PARAM_WRITE_CHECK_AMOUNT, this.cafe, this.transactionResults);

            } else {
                this.procWriteCheck.run(conn, custName, SmallBankConstants.PARAM_WRITE_CHECK_AMOUNT, this.transactionResults);
            }
        }

        if (!Config.CAFE)
            conn.commit();
        
        long endTime = System.nanoTime();
        if (Config.ENABLE_LOGGING) {
            generateLogRecord(startTime, endTime, transName);
            sequenceId++;
        }

        return TransactionStatus.SUCCESS;
    }

    public void generateLogRecord(long startTime, long endTime, String transName) {
        char transType = transName.charAt(0);       
        long custId0, custId1;    
        double cBal = 0d, sBal = 0d;
        
        char logType = ' ';
        ArrayList<Entity> es = new ArrayList<>();
        switch (transType) {
        case 'A':
            custId0 = (long)transactionResults.get(SmallBankConstants.SENDID);
            custId1 = (long)transactionResults.get(SmallBankConstants.DESTID);
            cBal = (double)transactionResults.get(SmallBankConstants.CHECKING_BAL);
            sBal = (double)transactionResults.get(SmallBankConstants.SAVINGS_BAL);
            double old_cBal = (double)transactionResults.get(SmallBankConstants.OLD_CHECKING_BAL);
            double old_sBal = (double)transactionResults.get(SmallBankConstants.OLD_SAVINGS_BAL);
            
            Property p0 = new Property(SmallBankConstants.BAL, String.valueOf(old_cBal), SmallBankConstants.VALUE_READ);
            Property p1 = new Property(SmallBankConstants.BAL, String.valueOf(cBal), SmallBankConstants.NEW_VALUE_UPDATE);
            Entity e0 = new Entity(String.valueOf(custId1), SmallBankConstants.ENTITY_CHECKING, new Property[] { p0, p1 });
            es.add(e0);
            
            p0 = new Property(SmallBankConstants.BAL, String.valueOf(old_sBal), SmallBankConstants.VALUE_READ);
            p1 = new Property(SmallBankConstants.BAL, String.valueOf(sBal), SmallBankConstants.NEW_VALUE_UPDATE);
            Entity e1 = new Entity(String.valueOf(custId0), SmallBankConstants.ENTITY_SAVINGS, new Property[] { p0, p1 });
            es.add(e1);
            
            logType = ValidationConstants.READ_WRITE_RECORD;
            break;
        case 'B':
            long custId = (long)transactionResults.get(SmallBankConstants.CUSTID);
            cBal = (double)transactionResults.get(SmallBankConstants.CHECKING_BAL);
            sBal = (double)transactionResults.get(SmallBankConstants.SAVINGS_BAL); 

            p0 = new Property(SmallBankConstants.BAL, String.valueOf(cBal), SmallBankConstants.VALUE_READ);
            e0 = new Entity(String.valueOf(custId), SmallBankConstants.ENTITY_CHECKING, new Property[] { p0 });
            es.add(e0);
            
            p0 = new Property(SmallBankConstants.BAL, String.valueOf(sBal), SmallBankConstants.VALUE_READ);
            e1 = new Entity(String.valueOf(custId), SmallBankConstants.ENTITY_SAVINGS, new Property[] { p0 });
            es.add(e1);
            
            logType = ValidationConstants.READ_RECORD;
            break;
        case 'D':
            custId = (long)transactionResults.get(SmallBankConstants.CUSTID);
            double amount = (double)transactionResults.get(SmallBankConstants.AMOUNT);
            p0 = new Property(SmallBankConstants.BAL, String.valueOf(amount), SmallBankConstants.INCREMENT_UPDATE);
            e0 = new Entity(String.valueOf(custId), SmallBankConstants.ENTITY_CHECKING, new Property[] {p0});
            es.add(e0);
            logType = ValidationConstants.UPDATE_RECORD;            
            break;
        case 'S':
            custId0 = (long)transactionResults.get(SmallBankConstants.SENDID);
            custId1 = (long)transactionResults.get(SmallBankConstants.DESTID);
            cBal = (double)transactionResults.get(SmallBankConstants.CHECKING_BAL);
            amount = (double)transactionResults.get(SmallBankConstants.AMOUNT);
            p0 = new Property(SmallBankConstants.BAL, String.valueOf(cBal), SmallBankConstants.VALUE_READ);
            p1 = new Property(SmallBankConstants.BAL, String.valueOf(-amount), SmallBankConstants.INCREMENT_UPDATE);
            e0 = new Entity(String.valueOf(custId0), SmallBankConstants.ENTITY_CHECKING, new Property[] {p0,p1});
            es.add(e0);
            
            p0 = new Property(SmallBankConstants.BAL, String.valueOf(amount), SmallBankConstants.INCREMENT_UPDATE);
            e1 = new Entity(String.valueOf(custId1), SmallBankConstants.ENTITY_CHECKING, new Property[] {p0});
            es.add(e1);
            logType = ValidationConstants.READ_WRITE_RECORD;
            break;
        case 'T':
            custId = (long)transactionResults.get(SmallBankConstants.CUSTID);
            sBal = (double)transactionResults.get(SmallBankConstants.SAVINGS_BAL);
            amount = (double)transactionResults.get(SmallBankConstants.AMOUNT);
            
            p0 = new Property(SmallBankConstants.BAL, String.valueOf(sBal), SmallBankConstants.VALUE_READ);
            p1 = new Property(SmallBankConstants.BAL, String.valueOf(-amount), SmallBankConstants.INCREMENT_UPDATE);
            e0 = new Entity(String.valueOf(custId), SmallBankConstants.ENTITY_SAVINGS, new Property[] {p0, p1});
            es.add(e0);
            logType = ValidationConstants.UPDATE_RECORD;
            break;
        case 'W':
            custId = (long)transactionResults.get(SmallBankConstants.CUSTID);
            cBal = (double)transactionResults.get(SmallBankConstants.CHECKING_BAL);
            amount = (double)transactionResults.get(SmallBankConstants.AMOUNT);
            
            p0 = new Property(SmallBankConstants.BAL, String.valueOf(cBal), SmallBankConstants.VALUE_READ);
            p1 = new Property(SmallBankConstants.BAL, String.valueOf(-amount), SmallBankConstants.INCREMENT_UPDATE);
            e1 = new Entity(String.valueOf(custId), SmallBankConstants.ENTITY_CHECKING, new Property[] { p0, p1 });
            es.add(e1);
            logType = ValidationConstants.READ_WRITE_RECORD;
            break;
        }
        
        String entities = Utilities.generateEntitiesLog(es);
        String log = Utilities.getLogString(logType, String.valueOf(transType), threadId, sequenceId, startTime, endTime, entities);
        
        if (logType == ValidationConstants.READ_RECORD) {
            this.readLog.append(log);
        } else {
            this.updateLog.append(log);
        }
        this.transactionResults.clear();
        
        try {
            if (readLog != null) {
                readLogAll.write(readLog.toString());
                readLog.delete(0, readLog.length());
            }
            if (updateLog != null) {
                updateLogAll.write(updateLog.toString());
                updateLog.delete(0, updateLog.length());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace(System.out);
        }
    }
}
