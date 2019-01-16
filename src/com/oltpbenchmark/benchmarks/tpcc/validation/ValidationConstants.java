package com.oltpbenchmark.benchmarks.tpcc.validation;

import java.text.DecimalFormat;

public class ValidationConstants {
	////////////// props
	public final static String APPLICATION_PROP = "app";
	public final static String INIT_PROP = "init";
	public final static String USE_KFKA_PROP = "kafka";
	public final static String GLOBAL_SCHEDULE_PROP = "globalschedule";
	public final static String ER_PROP = "er";
	public final static String FILE_LOG_PROP = "filelogdir";
	public final static String KAFKA_LOG_PROP = "kafkalogdir";
	public final static String KAFKA_SERVER_PROP = "kafkaserver";
	public final static int KAFKA_POLL_WAIT_MILLIS=200;
	public final static String CONSUMER_MAX_PARTITION_FETCH_BYTES="1024";
    public final static String CONSUMER_FETCH_MAX_WAIT_MS="500";
    public final static String CONSUMER_FETCH_MIN_BYTES="1";
    public static final int POLL_MAX_TRIES=10;
    public static final boolean PRODUCE_STATS=false;
    public static final boolean OS_STATS=true;
    public static final boolean POLL_DEBUG=false;

	public static String OSStatsFolder="/home/mr1/osstats";


	// =bg -p =false -p kafka=true -p globalschedule=false -p er= -p filelogdir= -p kafkalogdir=

	/////
	public static final boolean MR1Fix = true;
	public static boolean GLOBAL_SCHEDULE = false;
	public static boolean debugPrinter = false;
	public static boolean hasInitState = false;
	public static boolean countDiscardedWrites = true && !hasInitState;
	public static int THREAD_COUNT = 1;
	public static int THREAD_SEQ = 0;
	public final static int STATS_INTERVAL_SECONDS = 1;
	public static String dirSeparator = System.getProperty("file.separator");
	public static String lineSeparator = System.getProperty("line.separator");
	public static final boolean verbose = false;
	public static final boolean debug = false;
	public static final boolean staleHTML = false;
	// public static final int BG = 0;
	// public static final int TPCC = 1;
	// public static final int YCSB = 2;
	//
	// public static int Application = BG;

	public static final int MAX_WRITE_LOGS = -2;

	// ============ General =========================

	public static final char NEW_VALUE_UPDATE = 'N';
	public static final char INCREMENT_UPDATE = 'I';
	public static final char NO_READ_UPDATE = 'X';
	public static final char VALUE_READ = 'R';

	public static final char RECORD_ATTRIBUTE_SEPERATOR = ',';
	public static final char ENTITY_SEPERATOR = '&';
	public static final char PROPERY_SEPERATOR = '#';
	public static final char PROPERY_ATTRIBUTE_SEPERATOR = ':';
	public static final char ENTITY_ATTRIBUTE_SEPERATOR = ';';
	public static final char RELATIONSHIP_ENTITY_SEPERATOR = PROPERY_SEPERATOR;
	public static final char KEY_SEPERATOR = '-';

	public static final char UPDATE_RECORD = 'U';
	public static final char READ_RECORD = 'R';
	public static final char READ_WRITE_RECORD = 'Z';

	public static final String ENTITY_SEPERATOR_REGEX = "[" + ENTITY_SEPERATOR + "]";
	public static final String ENTITY_ATTRIBUTE_SEPERATOR_REGEX = "[" + ENTITY_ATTRIBUTE_SEPERATOR + "]";
	public static final String PROPERY_SEPERATOR_REGEX = "[" + PROPERY_SEPERATOR + "]";
	public static final String PROPERY_ATTRIBUTE_SEPERATOR_REGEX = "[" + PROPERY_ATTRIBUTE_SEPERATOR + "]";
	public static boolean USE_KAFKA = true;
	public static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("##.00");

	public static String[][] ENTITIES_INSERT_ACTIONS;

	// ============= TPCC ============================

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
	public static final String CUST_ORDER_REL = CUSTOMER_ENTITY + "*" + ORDER_ENTITY;

	public static final long DELIVERYDATE = 1444665544000L;
	public static final double ERROR_MARGIN = 0.03;

	public static final String CUSTOMER_BALANCE = "BALANCE";
	public static final String CUSTOMER_YTD_PAYMENT = "YTD_P";
	public static final String CUSTOMER_PAYMENT_COUNT = "P_CNT";

	public static final String ORDER_ORDER_COUNT = "OL_CNT";
	public static final String ORDER_CARRIER_ID = "CARRID";
	public static final String ORDER_DELIVERY_DATE = "OL_DEL_D";

	public static int DELIVERY_DATE_DIVISION = 1000; // This used to discard digits from the delivery date in millis to match retrieved time stamp;

	public static final String[] CUST_ORDER_REL_PROPERIES = { CUSTOMER_ENTITY, ORDER_ENTITY };
	public static final String[] CUSTOMER_PROPERIES = { CUSTOMER_BALANCE, CUSTOMER_YTD_PAYMENT, CUSTOMER_PAYMENT_COUNT };
	public static final String[] ORDER_PROPERIES = { ORDER_ORDER_COUNT, ORDER_CARRIER_ID, ORDER_DELIVERY_DATE };
	public static final String[] DISTRICT_PROPERIES = { "N_O_ID" };
	public static final String[] STOCK_PROPERIES = { "QUANTITY" };

	// ============= BG ==============================

	// ============= YCSB ============================

	public static final String USER_ENTITY = "USR";
	// =================KAFKA
	public static String KAFKA_SERVER = "10.0.0.240:9092";
	public static final boolean WINDOWS = false;
	public static final String PRINT_FREQ_PROP = "printfreq";
	public static final Object VALIDATOR_ID_PROP = "id";
	public static final Object NUM_VALIDATORS_PROP = "numvalidators";

	public static String erFile = null;

}
