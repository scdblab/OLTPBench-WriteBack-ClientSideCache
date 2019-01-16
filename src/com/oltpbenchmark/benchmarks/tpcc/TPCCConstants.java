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

import java.text.DecimalFormat;

public abstract class TPCCConstants {
    public static final String TABLENAME_DISTRICT = "DISTRICT";
    public static final String TABLENAME_WAREHOUSE = "WAREHOUSE";
    public static final String TABLENAME_ITEM = "ITEM";
    public static final String TABLENAME_STOCK = "STOCK";
    public static final String TABLENAME_CUSTOMER = "CUSTOMER";
    public static final String TABLENAME_HISTORY = "HISTORY";
    public static final String TABLENAME_OPENORDER = "OORDER";
    public static final String TABLENAME_ORDERLINE = "ORDER_LINE";
    public static final String TABLENAME_NEWORDER = "NEW_ORDER";

    public static final String NEWORDER_ACTION = "NO";
    public static final String ORDERSTATUS_ACTION = "OS";
    public static final String PAYMENT_ACTION = "PA";
    public static final String DELIVERY_ACTION = "DE";
    public static final String BUCKET_ACTION = "BU";
    public static final String STOCKLEVEL_ACTION = "SL";

    public static final String CUSTOMER_ENTITY = "CUS";// "CUSTOMER";
    public static final String ORDER_ENTITY = "ORD"; // ER";
    public static final String DISTRICT_ENTITY = "DIS";// TRICT";
    public static final String STOCK_ENTITY = "STK";

    public static final char NEW_VALUE_UPDATE = 'N';
    public static final char INCREMENT_UPDATE = 'I';
    public static final char NO_READ_UPDATE = 'X';
    public static final char VALUE_READ = 'R';
    public static final char KEY_SEPERATOR = '-';

    public static final String CACHE_POOL_NAME = "tpcc";
    public static final String TRACE_LOGGING_DIR = "tpcc_logs";
    public static final boolean STATS = true;
    public static boolean DML_Trace = false;
    public static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("##.00");

    public static final String USER_ENTITY = "USR";
    public static String[] ENTITY_NAMES = { USER_ENTITY };
    public static final String[] USER_PROPERIES = { "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10" };
    public static String[][] ENTITY_PROPERTIES = { USER_PROPERIES };

    public static final char RECORD_ATTRIBUTE_SEPERATOR = ',';
    public static final char ENTITY_SEPERATOR = '&';
    public static final char PROPERY_SEPERATOR = '#';
    public static final char PROPERY_ATTRIBUTE_SEPERATOR = ':';
    public static final char ENTITY_ATTRIBUTE_SEPERATOR = ';';
    public static final String ENTITY_SEPERATOR_REGEX = "[" + ENTITY_SEPERATOR + "]";
    public static final String ENTITY_ATTRIBUTE_SEPERATOR_REGEX = "[" + ENTITY_ATTRIBUTE_SEPERATOR + "]";
    public static final String PROPERY_SEPERATOR_REGEX = "[" + PROPERY_SEPERATOR + "]";
    public static final String PROPERY_ATTRIBUTE_SEPERATOR_REGEX = "[" + PROPERY_ATTRIBUTE_SEPERATOR + "]";

    public static final String CUSTOMER_BALANCE = "BALANCE";
    public static final String CUSTOMER_YTD_PAYMENT = "YTD_P";
    public static final String CUSTOMER_PAYMENT_COUNT = "P_CNT";

    public static final String ORDER_ORDER_COUNT = "OL_CNT";
    public static final String ORDER_CARRIER_ID = "CARRID";
    public static final String ORDER_DELIVERY_DATE = "OL_DEL_D";

    public static final double ERROR_MARGIN = 0.03;

    public static final char UPDATE_RECORD = 'U';
    public static final char READ_RECORD = 'R';

    public static boolean FILE_LOG = true;
    public static final String[] CUST_ORDER_REL_PROPERIES = { CUSTOMER_ENTITY, ORDER_ENTITY };
    public static final String[] CUSTOMER_PROPERIES = { CUSTOMER_BALANCE, CUSTOMER_YTD_PAYMENT, CUSTOMER_PAYMENT_COUNT };
    public static final String[] ORDER_PROPERIES = { ORDER_ORDER_COUNT, ORDER_CARRIER_ID, ORDER_DELIVERY_DATE };
    public static final String[] DISTRICT_PROPERIES = { "N_O_ID" };
    public static final String[] STOCK_PROPERIES = { "QUANTITY" };

    public static int DELIVERY_DATE_DIVISION = 1000; // This used to discard digits from the delivery date in millis to match retrieved time stamp;
    public static final String CUST_ORDER_REL = CUSTOMER_ENTITY + "*" + ORDER_ENTITY;

    public static final String KEY_CUSTWHSE_PRE = "k_CustWhse_w_d_c";
    public static final String KEY_CUSTWHSE = KEY_CUSTWHSE_PRE+",%s,%s,%s";
    public static final String KEY_DIST_PRE = "k_Dist_w_d";
    public static final String KEY_DIST = KEY_DIST_PRE+",%s,%s";
    public static final String KEY_DIST2_PRE = "k_Dist2_w_d";
    public static final String KEY_DIST2 = KEY_DIST2_PRE+",%s,%s";
    public static final String KEY_CUSTOMERID_ORDER_PRE = "k_CustomerIDOrder_w_d_o";
    public static final String KEY_CUSTOMERID_ORDER = KEY_CUSTOMERID_ORDER_PRE+",%s,%s,%s";
    public static final String KEY_LAST_ORDER_PRE = "k_LastOrder_w_d_c";
    public static final String KEY_LAST_ORDER = KEY_LAST_ORDER_PRE+",%s,%s,%s";
    public static final String KEY_ITEM_PRE = "k_Item_i";
    public static final String KEY_ITEM = KEY_ITEM_PRE+",%s";
    public static final String KEY_STOCK_PRE = "k_Stk_w_i";
    public static final String KEY_STOCK = KEY_STOCK_PRE+",%s,%s";
    public static final String KEY_WAREHOUSE_PRE = "k_Whse_w";
    public static final String KEY_WAREHOUSE = KEY_WAREHOUSE_PRE+",%s";
    public static final String KEY_CUSTOMERID_PRE = "k_CustID_w_d_c";
    public static final String KEY_CUSTOMERID = KEY_CUSTOMERID_PRE+",%s,%s,%s";
    public static final String KEY_CUSTOMERS_BY_NAME_PRE = "k_CustByName_w_d_l";
    public static final String KEY_CUSTOMERS_BY_NAME = KEY_CUSTOMERS_BY_NAME_PRE+",%s,%s,%s";
    
    public static final String KEY_CUSTOMER_DATA_PRE = "k_CustData_w_d_c";
    public static final String KEY_CUSTOMER_DATA = KEY_CUSTOMER_DATA_PRE+",%s,%s,%s";
    public static final String KEY_ORDER_LINES_PRE = "k_Ol_w_d_o";
    public static final String KEY_ORDER_LINES = KEY_ORDER_LINES_PRE+",%s,%s,%s";
    public static final String KEY_OL_AMOUNT_PRE = "k_OlAmount_w_d_o";
    public static final String KEY_OL_AMOUNT = KEY_OL_AMOUNT_PRE+",%s,%s,%s";
    public static final String KEY_NEW_ORDER_IDS_PRE = "k_NewOrderIds_w_d";
    public static final String KEY_NEW_ORDER_IDS = KEY_NEW_ORDER_IDS_PRE+",%s,%s";
    public static final String KEY_STOCK_LAST20ORDERS_ITEM_IDS_PRE = "k_StkItemIds_w_d";
    public static final String KEY_STOCK_LAST20ORDERS_ITEM_IDS = KEY_STOCK_LAST20ORDERS_ITEM_IDS_PRE+",%s,%s";
    public static final String KEY_STOCK_ITEMS_EQUAL_THRESHOLD_PRE = "k_StkItThreshold_w_T";
    public static final String KEY_STOCK_ITEMS_EQUAL_THRESHOLD = KEY_STOCK_ITEMS_EQUAL_THRESHOLD_PRE+",%s,%s";

    public static final String DATA_ITEM_DISTRICT_PRE = "D_w_d";
    public static final String DATA_ITEM_DISTRICT = DATA_ITEM_DISTRICT_PRE+",%s,%s";
    public static final String DATA_ITEM_NEW_ORDER_PRE = "NO_w_d";
    public static final String DATA_ITEM_NEW_ORDER = DATA_ITEM_NEW_ORDER_PRE+",%s,%s";
    public static final String DATA_ITEM_ORDER_PRE = "O_w_d";
    public static final String DATA_ITEM_ORDER = DATA_ITEM_ORDER_PRE+",%s,%s";
    public static final String DATA_ITEM_ORDER_LINE_PRE = "OL_w_d_o";
    public static final String DATA_ITEM_ORDER_LINE = DATA_ITEM_ORDER_LINE_PRE+",%s,%s,%s";
    public static final String DATA_ITEM_STOCK_PRE = "S_w_i";
    public static final String DATA_ITEM_STOCK = DATA_ITEM_STOCK_PRE+",%s,%s";
    public static final String DATA_ITEM_WAREHOUSE_PRE = "W_w";
    public static final String DATA_ITEM_WAREHOUSE = DATA_ITEM_WAREHOUSE_PRE+",%s";
    public static final String DATA_ITEM_CUSTOMER_PRE = "C_w_d_c";
    public static final String DATA_ITEM_CUSTOMER = DATA_ITEM_CUSTOMER_PRE+",%s,%s,%s";
    public static final String DATA_ITEM_HISTORY_PRE = "H_w_d_c";
    public static final String DATA_ITEM_HISTORY = DATA_ITEM_HISTORY_PRE+",%s,%s,%s";
}
