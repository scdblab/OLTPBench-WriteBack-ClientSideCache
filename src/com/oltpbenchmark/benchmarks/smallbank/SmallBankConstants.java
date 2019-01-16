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

package com.oltpbenchmark.benchmarks.smallbank;

import java.text.DecimalFormat;

public abstract class SmallBankConstants {

    // ----------------------------------------------------------------
    // TABLE NAMES
    // ----------------------------------------------------------------
    public static final String TABLENAME_ACCOUNTS   = "ACCOUNTS";
    public static final String TABLENAME_SAVINGS    = "SAVINGS";
    public static final String TABLENAME_CHECKING   = "CHECKING";
    
    public static final int BATCH_SIZE              = 5000;
    
    // ----------------------------------------------------------------
    // ACCOUNT INFORMATION
    // ----------------------------------------------------------------
    
    // Default number of customers in bank
    public static final int NUM_ACCOUNTS            = 1000000;
    
    public static final boolean HOTSPOT_USE_FIXED_SIZE  = false;
    public static final double HOTSPOT_PERCENTAGE       = 25; // [0% - 100%]
    public static final int HOTSPOT_FIXED_SIZE          = 100; // fixed number of tuples
    
    // ----------------------------------------------------------------
    // ADDITIONAL CONFIGURATION SETTINGS
    // ----------------------------------------------------------------
    
    // Initial balance amount
    // We'll just make it really big so that they never run out of money
    public static final int MIN_BALANCE             = 10000;
    public static final int MAX_BALANCE             = 50000;
    
    // ----------------------------------------------------------------
    // PROCEDURE PARAMETERS
    // These amounts are from the original code
    // ----------------------------------------------------------------
    public static final double PARAM_SEND_PAYMENT_AMOUNT = 5.0d;
    public static final double PARAM_DEPOSIT_CHECKING_AMOUNT = 1.3d;
    public static final double PARAM_TRANSACT_SAVINGS_AMOUNT = 20.20d;
    public static final double PARAM_WRITE_CHECK_AMOUNT = 5.0d;
    
    
    // for Polygraph
    public static final String NAME = "name";
    public static final String CUSTID = "custid";
    public static final String SENDID = "sendid";
    public static final String DESTID = "destid";
    public static final String BAL = "bal";
    public static final String CHECKING_BAL = "cbal";
    public static final String SAVINGS_BAL = "sbal";
    public static final String OLD_CHECKING_BAL = "old_cbal";
    public static final String OLD_SAVINGS_BAL = "old_sbal";
    
    public static final String AMOUNT = "amount";
    
    public static final String[] ACCOUNTS_PROPS = { NAME, CUSTID };
    public static final String[] CHECKING_PROPS = { CUSTID, BAL };
    public static final String[] SAVINGS_PROPS = { CUSTID, BAL };
    
    public static final char RECORD_ATTRIBUTE_SEPERATOR = ',';
    public static final char ENTITY_SEPERATOR = '&';
    public static final char PROPERY_SEPERATOR = '#';
    public static final char PROPERY_ATTRIBUTE_SEPERATOR = ':';
    public static final char ENTITY_ATTRIBUTE_SEPERATOR = ';';
    public static final char RELATIONSHIP_ENTITY_SEPERATOR = PROPERY_SEPERATOR;
    public static final char KEY_SEPERATOR = '-';
    
    public static final String ENTITY_CHECKING = "CHECKING";
    public static final String ENTITY_SAVINGS = "SAVINGS";
    public static final String[] ENTITY_NAMES = new String[] { "ACCOUNTS", "CHECKING", "SAVINGS" };
    public static final String[][] ENTITY_PROPERTIES = { ACCOUNTS_PROPS, CHECKING_PROPS, SAVINGS_PROPS };
    public static final String TRACE_LOGGING_DIR = "smallbank_logs";
    
    public static final char NEW_VALUE_UPDATE = 'N';
    public static final char INCREMENT_UPDATE = 'I';
    public static final char NO_READ_UPDATE = 'X';
    public static final char VALUE_READ = 'R';
    
    public static final double ERROR_MARGIN = 0.03;
    public static boolean STATS = true;
    public static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("##.00");
    
    public static final String QUERY_ACCOUNT_PREFIX = "Q_ACCOUNT_NAME";
    public static final String QUERY_ACCOUNT = QUERY_ACCOUNT_PREFIX+",%s";
    
    public static final String QUERY_ACCOUNT_BY_CUSTID_PREFIX = "Q_ACCOUNT_ID";
    public static final String QUERY_ACCOUNT_BY_CUSTID = QUERY_ACCOUNT_BY_CUSTID_PREFIX+",%d";

    public static final String QUERY_CHECKING_PREFIX = "Q_CHECKING";
    public static final String QUERY_SAVINGS_PREFIX = "Q_SAVINGS";
    public static final String QUERY_CHECKING_BAL = QUERY_CHECKING_PREFIX+",%d";
    public static final String QUERY_SAVINGS_BAL = QUERY_SAVINGS_PREFIX+",%d";

    public static final String UPDATE_CHECKING_PREFIX = "U_CHECKING";
    public static final String UPDATE_SAVINGS_PREFIX = "U_SAVINGS";

    public static final String UPDATE_INCR_CHECKING_BAL = UPDATE_CHECKING_PREFIX+",%d,incr,%.2f";
    public static final String UPDATE_DECR_CHECKING_BAL = UPDATE_CHECKING_PREFIX+",%d,decr,%.2f";

    public static final String UPDATE_DECR_SAVINGS_BAL = UPDATE_SAVINGS_PREFIX+",%d,decr,%.2f";
    public static final String UPDATE_SET_SAVINGS_BAL = UPDATE_SAVINGS_PREFIX+",%d,set,%.2f";
    public static final String UPDATE_SET_CHECKING_BAL = UPDATE_CHECKING_PREFIX+",%d,set,%.2f";

}
