/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.tpcc;

/*
 * jTPCCConfig - Basic configuration parameters for jTPCC
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.text.SimpleDateFormat;

public final class TPCCConfig {

    public static enum TransactionType {
        INVALID, // Exists so the order is the same as the constants below
        NEW_ORDER, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL
    }

    public final static String[] nameTokens = { "BAR", "OUGHT", "ABLE", "PRI",
        "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };

    public final static String terminalPrefix = "Term-";

    public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public final static int configCommitCount = 1000; // commit every n records
    public final static int configWhseCount = 1;
    public final static int configItemCount = 100000; // tpc-c std = 100,000
    public static int configDistPerWhse = 10; // tpc-c std = 10
    public static int configCustPerDist = 3000; // tpc-c std = 3,000

    /** An invalid item id used to rollback a new order transaction. */
    public static final int INVALID_ITEM_ID = -12345;

    public static final String QUERY_DISTRICT_NEXT_ORDER_PRE="QUERY_DISTRICT_NEXT_ORDER";
    public static final String QUERY_DISTRICT_NEXT_ORDER = QUERY_DISTRICT_NEXT_ORDER_PRE+",%d,%d";

    public static final String QUERY_GET_COUNT_STOCK_PRE="QUERY_GET_COUNT_STOCK";
    public static final String QUERY_GET_COUNT_STOCK = QUERY_GET_COUNT_STOCK_PRE+",%d,%d,%d,%d";
    // split into two queries
    public static final String QUERY_STOCK_GET_ITEM_IDS_PRE ="QUERY_STOCK_GET_ITEM_IDS";
    public static final String QUERY_STOCK_GET_ITEM_IDS = QUERY_STOCK_GET_ITEM_IDS_PRE+",%d,%d,%d";
    public static final String QUERY_STOCK_COUNT_ITEMS_IDS_PRE ="QUERY_STOCK_COUNT_ITEMS_IDS";
    public static final String QUERY_STOCK_COUNT_ITEMS_IDS = QUERY_STOCK_COUNT_ITEMS_IDS_PRE+",%d,%d,%s";
    public static final String QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD_PRE ="QUERY_STOCK_ITEMS_EQUAL_THRESHOLD_IDS";
    public static final String QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD = QUERY_STOCK_ITEMS_IDS_EQUAL_THRESHOLD_PRE+",%d,%d";

    public static final String QUERY_PAY_GET_CUST_PRE = "QUERY_PAY_GET_CUST";
    public static final String QUERY_PAY_GET_CUST = QUERY_PAY_GET_CUST_PRE+",%d,%d,%d";

    public static final String QUERY_PAY_GET_CUST_BY_NAME_PRE = "QUERY_PAY_GET_CUST_BY_NAME";
    public static final String QUERY_PAY_GET_CUST_BY_NAME = QUERY_PAY_GET_CUST_BY_NAME_PRE+",%d,%d,%s";

    public static final String QUERY_ORDER_STAT_GET_ORDER_LINES_PRE = "QUERY_ORDER_STAT_GET_ORDER_LINES";
    public static final String QUERY_ORDER_STAT_GET_ORDER_LINES = QUERY_ORDER_STAT_GET_ORDER_LINES_PRE+",%d,%d,%d";

    public static final String QUERY_ORDER_STAT_GET_NEWEST_ORDER_PRE = "QUERY_ORDER_STAT_GET_NEWEST_ORDER";
    public static final String QUERY_ORDER_STAT_GET_NEWEST_ORDER = QUERY_ORDER_STAT_GET_NEWEST_ORDER_PRE+",%d,%d,%d";

    public static final String QUERY_GET_CUST_WHSE_PRE = "QUERY_GET_CUST_WHSE";
    public static final String QUERY_GET_CUST_WHSE = QUERY_GET_CUST_WHSE_PRE+",%d,%d,%d";

//    public static final String QUERY_GET_DIST_PRE = "QUERY_GET_DIST";
//    public static final String QUERY_GET_DIST = QUERY_GET_DIST_PRE+",%d,%d";

    public static final String QUERY_GET_DIST2_PRE = "QUERY_GET_DIST2";
    public static final String QUERY_GET_DIST2 = QUERY_GET_DIST2_PRE+",%d,%d";

    public static final String QUERY_GET_ITEM_PRE = "QUERY_GET_ITEM";
    public static final String QUERY_GET_ITEM = QUERY_GET_ITEM_PRE+",%d";

    public static final String QUERY_GET_STOCK_PRE = "QUERY_GET_STOCK";
    public static final String QUERY_GET_STOCK = QUERY_GET_STOCK_PRE+",%d,%d";
    
    public static final String QUERY_RANGE_GET_ITEMS_PRE = "QUERY_RANGE_GET_ITEMS";
    public static final String QUERY_RANGE_GET_ITEMS = QUERY_RANGE_GET_ITEMS_PRE+",%d,%d,%d";

    public static final String DML_UPDATE_STOCK_PRE = "DML_UPDATE_STOCK";
    public static final String DML_UPDATE_STOCK = DML_UPDATE_STOCK_PRE+",%d,%d,%d,%d,%d,%d,%d";

    public static final String DML_INSERT_ORDER_LINE_PRE = "DML_INSERT_ORDER_LINE";
    public static final String DML_INSERT_ORDER_LINE = DML_INSERT_ORDER_LINE_PRE+",%d,%d,%d,%d,%d,%d,%d,%s,%s";

    public static final String DML_UPDATE_DIST_PRE = "DML_UPDATE_DIST";
    public static final String DML_UPDATE_DIST = DML_UPDATE_DIST_PRE+",%d,%d,%d";

    public static final String DML_INSERT_OORDER_PRE = "DML_INSERT_OORDER";
    public static final String DML_INSERT_OORDER = DML_INSERT_OORDER_PRE+",%d,%d,%d,%d,%d,%d,%d";

    public static final String DML_INSERT_NEW_ORDER_PRE = "DML_INSERT_NEW_ORDER";
    public static final String DML_INSERT_NEW_ORDER = DML_INSERT_NEW_ORDER_PRE+",%d,%d,%d";

    public static final String QUERY_GET_ORDER_ID_PRE = "QUERY_GET_ORDER_ID";
    public static final String QUERY_GET_ORDER_ID = QUERY_GET_ORDER_ID_PRE+",%d,%d";

    public static final String QUERY_GET_WHSE_PRE = "QUERY_GET_WHSE";
    public static final String QUERY_GET_WHSE = QUERY_GET_WHSE_PRE+",%d";

    public static final String DML_DELETE_NEW_ORDER_PRE = "DML_DELETE_NEW_ORDER";
    public static final String DML_DELETE_NEW_ORDER = DML_DELETE_NEW_ORDER_PRE+",%d,%d,%d";

    public static final String QUERY_DELIVERY_GET_CUST_ID_PRE = "QUERY_DELIVERY_GET_CUST_ID";
    public static final String QUERY_DELIVERY_GET_CUST_ID = QUERY_DELIVERY_GET_CUST_ID_PRE+",%d,%d,%d";

    public static final String DML_UPDATE_CARRIER_ID_PRE = "DML_UPDATE_CARRIER_ID";
    public static final String DML_UPDATE_CARRIER_ID = DML_UPDATE_CARRIER_ID_PRE+",%d,%d,%d,%d,%d";

    public static final String DML_UPDATE_DELIVERY_DATE_PRE = "DML_UPDATE_DELIVERY_DATE";
    public static final String DML_UPDATE_DELIVERY_DATE = DML_UPDATE_DELIVERY_DATE_PRE+",%d,%d,%d,%d";

    public static final String QUERY_GET_SUM_ORDER_AMOUNT_PRE = "QUERY_GET_SUM_ORDER_AMOUNT";
    public static final String QUERY_GET_SUM_ORDER_AMOUNT = QUERY_GET_SUM_ORDER_AMOUNT_PRE+",%d,%d,%d";

    public static final String DML_UPDATE_CUST_BAL_DELIVERY_CNT_PRE = "DML_UPDATE_CUST_BAL_DELIVERY_CNT";
    public static final String DML_UPDATE_CUST_BAL_DELIVERY_CNT = DML_UPDATE_CUST_BAL_DELIVERY_CNT_PRE+",%d,%d,%d,%s";

    public static final String DML_UPDATE_WHSE_PRE = "DML_UPDATE_WHSE";
    public static final String DML_UPDATE_WHSE = DML_UPDATE_WHSE_PRE+",%d,%s";

    public static final String DML_PAY_UPDATE_DIST_PRE = "DML_PAY_UPDATE_DIST";
    public static final String DML_PAY_UPDATE_DIST = DML_PAY_UPDATE_DIST_PRE+",%d,%d,%.2f";

    public static final String QUERY_GET_CUST_C_DATA_PRE = "QUERY_GET_CUST_C_DATA";
    public static final String QUERY_GET_CUST_C_DATA = QUERY_GET_CUST_C_DATA_PRE+",%d,%d,%d";

    public static final String DML_UPDATE_CUST_BAL_C_DATA_PRE = "DML_UPDATE_CUST_BAL_C_DATA";
    public static final String DML_UPDATE_CUST_BAL_C_DATA = DML_UPDATE_CUST_BAL_C_DATA_PRE+",%d,%d,%d,%s,%.2f,%d,%s";

    public static final String DML_UPDATE_CUST_BAL_PRE = "DML_UPDATE_CUST_BAL";
    public static final String DML_UPDATE_CUST_BAL = DML_UPDATE_CUST_BAL_PRE+",%d,%d,%d,%s,%.2f,%d";

    public static final String DML_INSERT_HISTORY_PRE = "DML_INSERT_HISTORY";
    public static final String DML_INSERT_HISTORY = DML_INSERT_HISTORY_PRE+",%d,%d,%d,%d,%d,%d,%.2f,%s";

    public static final boolean useRangeQC = true;
    public static final boolean crossWarehouse = false;
    
    public static boolean warmup=false;
}
