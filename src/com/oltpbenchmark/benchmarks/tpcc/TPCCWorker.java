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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


/*
 * jTPCCTerminal - Terminal emulator code for jTPCC (transactions)
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;
import com.oltpbenchmark.benchmarks.tpcc.validation.Entity;
import com.oltpbenchmark.benchmarks.tpcc.validation.Property;
import com.oltpbenchmark.benchmarks.tpcc.validation.Utilities;
import com.oltpbenchmark.benchmarks.tpcc.validation.ValidationConstants;
import com.oltpbenchmark.types.TransactionStatus;
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;

public class TPCCWorker extends Worker<TPCCBenchmark> {

    private static final Logger LOG = Logger.getLogger(TPCCWorker.class);

	private final int terminalWarehouseID;
	/** Forms a range [lower, upper] (inclusive). */
	private final int terminalDistrictLowerID;
	private final int terminalDistrictUpperID;
	// private boolean debugMessages;
	
	private final Random gen = new Random(7);
	
    public NgCache cafe = null;
    
    private int threadId;
    private int sequenceId = 0;
    
    public HashMap<String, Object> transactionResults = null;
    public BufferedWriter updateLogAll = null;
    public BufferedWriter readLogAll = null;
    private StringBuilder readLog = null;
    private StringBuilder updateLog = null;
    
    private StringBuilder stkLog = null;
    public BufferedWriter stkLogAll = null;
    
    public int transactionCount = 0;

	private int numWarehouses;

	public TPCCWorker(TPCCBenchmark benchmarkModule, int id,
			int terminalWarehouseID, int terminalDistrictLowerID,
			int terminalDistrictUpperID, int numWarehouses)
			throws SQLException {
		super(benchmarkModule, id);
		
		this.terminalWarehouseID = terminalWarehouseID;
		System.out.println("Worker "+id+" processes for warehouse "+terminalWarehouseID);
		
		this.terminalDistrictLowerID = terminalDistrictLowerID;
		this.terminalDistrictUpperID = terminalDistrictUpperID;
		assert this.terminalDistrictLowerID >= 1;
		assert this.terminalDistrictUpperID <= TPCCConfig.configDistPerWhse;
		assert this.terminalDistrictLowerID <= this.terminalDistrictUpperID;
		this.numWarehouses = numWarehouses;
		
		this.threadId = id;
		
        if (Config.CAFE) {
            CacheStore cacheStore = new TPCCCacheStore(conn);
            WriteBack cacheBack = new TPCCWriteBack(conn, terminalWarehouseID);
            Stats stats = Stats.getStatsInstance(threadId);
            if (TPCCConstants.STATS)
                Stats.stats = true;
            cafe = new NgCache(cacheStore, cacheBack, TPCCConstants.CACHE_POOL_NAME, 
            		Config.CACHE_POLICY, Config.NUM_AR_WORKERS, stats, this.benchmarkModule.workConf.getDBConnection(),
            		this.benchmarkModule.workConf.getDBUsername(), this.benchmarkModule.workConf.getDBPassword(), true, Config.AR_SLEEP,
            		this.benchmarkModule.workConf.getMinWarehouseId(), this.benchmarkModule.workConf.getMaxWarehouseId());
            this.cafe.setBatch(Config.BATCH);
            System.out.println("Cache Policy: "+Config.CACHE_POLICY);
        }

        if (Config.ENABLE_LOGGING) {
            try {
                String dir = TPCCConstants.TRACE_LOGGING_DIR;
                File ufile = new File(dir + "/update0-" + threadId + ".txt");
                FileWriter ufstream = new FileWriter(ufile);
                updateLogAll = new BufferedWriter(ufstream);
                // read file
                File rfile = new File(dir + "/read0-" + threadId + ".txt");
                FileWriter rfstream = new FileWriter(rfile);
                readLogAll = new BufferedWriter(rfstream);
                readLog = new StringBuilder();
                updateLog = new StringBuilder();
                
                File sfile = new File(dir+"/stk-"+threadId+".txt");
                FileWriter sfstream = new FileWriter(sfile);
                stkLogAll = new BufferedWriter(sfstream);
                stkLog = new StringBuilder();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            transactionResults = new HashMap<String, Object>();
        }
	}

	/**
	 * Executes a single TPCC transaction of type transactionType.
	 */
	@Override
    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
		long startTime = System.nanoTime();
		String transName = nextTransaction.getName();
        try {
            TPCCProcedure proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());
//            System.out.println(nextTransaction.getProcedureClass().getName());
            
			if (Config.ENABLE_LOGGING)
				this.transactionResults.clear();
			
            if (Config.CAFE) {
            	proc.run(conn, gen, terminalWarehouseID, numWarehouses,
            			terminalDistrictLowerID, terminalDistrictUpperID, cafe, transactionResults);   
                transactionCount++;
            } else  {
            	proc.run(conn, gen, terminalWarehouseID, numWarehouses,
            			terminalDistrictLowerID, terminalDistrictUpperID, transactionResults);
            }
        } catch (ClassCastException ex){
            //fail gracefully
        	LOG.error("We have been invoked with an INVALID transactionType?!");
        	throw new RuntimeException("Bad transaction type = "+ nextTransaction);
	    } catch (RuntimeException ex) {
			if (transName.length() >= 2) {
				String tN = transName.substring(0, 2);
				System.out.println(this.threadId + ": " + tN + ": conn.rollback()");
			} else {
				System.out.println(this.threadId + ": " + transName + ": conn.rollback()");
			}
			conn.rollback();
			Stats.getStatsInstance(threadId).incr("aborted_transactions");
			return (TransactionStatus.RETRY_DIFFERENT);
		} catch (SQLException ex) {
		    if (transName.length() >= 2) {
                String tN = transName.substring(0, 2);
                System.out.println(this.threadId + ": " + tN + ": conn.rollback()");
            } else {
                System.out.println(this.threadId + ": " + transName + ": conn.rollback()");
            }
		    Stats.getStatsInstance(threadId).incr("aborted_transactions");
		    throw ex;
		}

        if (!Config.CAFE) {
        	conn.commit();
        	Stats.getStatsInstance(threadId).incr("committed_transactions");
            transactionCount++;
        }
        
        long endTime = System.nanoTime();
        if (Config.ENABLE_LOGGING) {
            generateLogRecord(startTime, endTime, nextTransaction.getName());
            sequenceId++;
        }
        
        return (TransactionStatus.SUCCESS);
	}

	private void generateLogRecord(long startTime, long endTime, String transName) {
		char updateType = 'N';
		try {

			// System.out.println("####" + transName);
			char firstLetter = transName.toUpperCase().charAt(0);
			String operationId = null;
			switch (firstLetter) {
			case 'O':
				// Set<String> aaaaaaa = transactionResults.keySet();
				// for (String s : aaaaaaa) {
				// System.out.print("[" + s + "," + transactionResults.get(s) + "], ");
				// }
				// System.out.println();
				String districtId;
				int w_id = (Integer) this.transactionResults.get("w_id");
				int d_id = (Integer) this.transactionResults.get("d_id");
				int c_id = (Integer) this.transactionResults.get("c_id");
				int o_id = (Integer) this.transactionResults.get("o_id");
				float c_balance = (float) this.transactionResults.get("c_balance");
				int c_payment_cnt = (Integer) this.transactionResults.get("c_payment_cnt");
				float c_ytd_payment = (Float) this.transactionResults.get("c_ytd_payment");
				int ol_count = (Integer) this.transactionResults.get("ol_count");
				int o_carrier_id = (Integer) this.transactionResults.get("o_carrier_id");
				long readMillis = (Long) this.transactionResults.get("ol_delivery_d");

				String customerId = generateID(w_id, d_id, c_id);
				String orderId = generateID(w_id, d_id, c_id, o_id);

				if (TPCCConstants.FILE_LOG) {
					long ol_delivery_d = readMillis / TPCCConstants.DELIVERY_DATE_DIVISION;

					// c_balance = Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(c_balance));
					// c_ytd_payment = Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(c_ytd_payment));

					// String customerProperties = Utilities.getPropertiesString(TPCCConstants.CUSTOMER_PROPERIES, c_balance, TPCCConstants.VALUE_READ, c_ytd_payment, TPCCConstants.VALUE_READ, c_payment_cnt, TPCCConstants.VALUE_READ, orderId, TPCCConstants.VALUE_READ);
					// String orderProperties = Utilities.getPropertiesString(TPCCConstants.ORDER_PROPERIES, ol_count, TPCCConstants.VALUE_READ, o_carrier_id, TPCCConstants.VALUE_READ, ol_delivery_d, TPCCConstants.VALUE_READ);
					//
					// String customerLogString = Utilities.getEntityLogString(TPCCConstants.CUSTOMER_ENTITY, customerId, customerProperties);
					// String orderLogString = Utilities.getEntityLogString(TPCCConstants.ORDER_ENTITY, orderId, orderProperties);
					String relatioshipId = customerId;// Utilities.concat(customerId, TPCCConstants.KEY_SEPERATOR, orderId);

					String logCustomer = get_OrderStatus_Customer_String(customerId, c_balance, c_ytd_payment, c_payment_cnt);
					String logCustOrdRelationship = get_OrderStatus_CustOrdRelationship_String(relatioshipId, customerId, orderId);
					String logOrder = get_OrderStatus_Order_String(orderId, ol_count, o_carrier_id, ol_delivery_d);

					String ReadLogString = Utilities.getLogString(TPCCConstants.READ_RECORD, TPCCConstants.ORDERSTATUS_ACTION, threadId, sequenceId, startTime, endTime, logCustomer, logOrder, logCustOrdRelationship);
					this.readLog.append(ReadLogString);


				} else {
					// System.out.println("Customer ID:" + customerId);
					double expectedV = 0;
					double readV = 0;
					// if (ValidationMain.dbState.get(TPCCConstants.CUSTOMER_ENTITY).get(customerId) == null) {
					// System.out.println("Customer ID:" + customerId + " not Exist!!! Exiting ...");
					// System.exit(0);
					// }
					// expectedV = Double.parseDouble(ValidationMain.dbState.get(TPCCConstants.CUSTOMER_ENTITY).get(customerId).get(0).getProperties()[0].getValue());

					// readV = c_balance;
					operationId = "CBLNC";
					// readV = Double.valueOf(TPCCConstants.DECIMAL_FORMAT.format(readV));
					// expectedV = Double.valueOf(TPCCConstants.DECIMAL_FORMAT.format(expectedV));
					if (Math.abs(expectedV - readV) > TPCCConstants.ERROR_MARGIN) {
						printStale(expectedV, readV, customerId, operationId, TPCCConstants.ORDERSTATUS_ACTION);
					}

					// expectedV = Double.parseDouble(ValidationMain.dbState.get(TPCCConstants.CUSTOMER_ENTITY).get(customerId).get(0).getProperties()[2].getValue());
					readV = c_payment_cnt;
					operationId = "CPCNT";
					if (expectedV != readV) {
						printStale(expectedV, readV, customerId, operationId, TPCCConstants.ORDERSTATUS_ACTION);
					}

					// expectedV = Double.parseDouble(ValidationMain.dbState.get(TPCCConstants.CUSTOMER_ENTITY).get(customerId).get(0).getProperties()[1].getValue());
					readV = c_ytd_payment;
					operationId = "CPYTD";

					// readV = Double.valueOf(TPCCConstants.DECIMAL_FORMAT.format(readV));
					// expectedV = Double.valueOf(TPCCConstants.DECIMAL_FORMAT.format(expectedV));
					if (Math.abs(expectedV - readV) > TPCCConstants.ERROR_MARGIN) {
						printStale(expectedV, readV, customerId, operationId, TPCCConstants.ORDERSTATUS_ACTION);
					}

					// expectedV = Double.parseDouble(ValidationMain.dbState.get(TPCCConstants.CUSTOMER_ENTITY).get(customerId).get(0).getProperties()[3].getValue());
					readV = o_id;
					operationId = "COID";
					if (expectedV != readV) {
						printStale(expectedV, readV, customerId, operationId, TPCCConstants.ORDERSTATUS_ACTION);
					}
					// expectedV = Double.parseDouble(ValidationMain.dbState.get(TPCCConstants.ORDER_ENTITY).get(orderId).get(0).getProperties()[1].getValue());
					readV = o_carrier_id;
					operationId = "OCARID";
					if (expectedV != readV) {
						printStale(expectedV, readV, customerId, operationId, TPCCConstants.ORDERSTATUS_ACTION);
					}

					// expectedV = Double.parseDouble(ValidationMain.dbState.get(TPCCConstants.ORDER_ENTITY).get(orderId).get(0).getProperties()[0].getValue());
					readV = ol_count;
					boolean checkOrderLines = true;
					operationId = "OLCNT";
					if (expectedV != readV) {
						checkOrderLines = false;
						printStale(expectedV, readV, customerId, operationId, TPCCConstants.ORDERSTATUS_ACTION);
					}
					if (checkOrderLines) {
						// for (int i = 0; i < ol_count; i++) {

						// long readMillis = (Long)
						// this.transactionResults.get(Integer.toString(0));
						long expectedMillis = 0;// Long.parseLong(ValidationMain.dbState.get(TPCCConstants.ORDER_ENTITY).get(orderId).get(0).getProperties()[2].getValue());

						long ev = expectedMillis / TPCCConstants.DELIVERY_DATE_DIVISION;
						long rv = readMillis / TPCCConstants.DELIVERY_DATE_DIVISION;

						if (rv != ev) {
							printStale(ev, rv, customerId, operationId, TPCCConstants.ORDERSTATUS_ACTION);
						}

						// }

					}
				}
				break;

			case 'N':
				w_id = (Integer) this.transactionResults.get("w_id");
				d_id = (Integer) this.transactionResults.get("d_id");
				c_id = (Integer) this.transactionResults.get("c_id");
				o_id = (Integer) this.transactionResults.get("o_id");
				ol_count = (Integer) this.transactionResults.get("ol_count");
				o_carrier_id = (Integer) this.transactionResults.get("o_carrier_id");
				long ol_delivery_d = (Long) this.transactionResults.get("ol_delivery_d");

				int d_next_o_id = (Integer) this.transactionResults.get("d_next_o_id");

				orderId = generateID(w_id, d_id, c_id, o_id);
				customerId = generateID(w_id, d_id, c_id);
				districtId = generateID(w_id, d_id);

				if (TPCCConstants.FILE_LOG) {
					ol_delivery_d = ol_delivery_d / TPCCConstants.DELIVERY_DATE_DIVISION;

					// String orderPropertiesString = Utilities.getPropertiesString(TPCCConstants.ORDER_PROPERIES, ol_count, TPCCConstants.NEW_VALUE_UPDATE, o_carrier_id, TPCCConstants.NEW_VALUE_UPDATE, ol_delivery_d, TPCCConstants.NEW_VALUE_UPDATE);
					// String districtPropertiesString = Utilities.getPropertiesString(TPCCConstants.DISTRICT_PROPERIES, 1, TPCCConstants.INCREMENT_UPDATE);

					String relatioshipId = customerId;// Utilities.concat(customerId, TPCCConstants.KEY_SEPERATOR, orderId);

					String logCustOrdRelationship = get_NewOrder_CustOrdRelationship_String(relatioshipId, customerId, orderId);
					String logOrder = get_NewOrder_Order_String(orderId, ol_count, o_carrier_id, ol_delivery_d);
					String logDist = "Dist;" + districtId + ";d_next_o_id:" + d_next_o_id + ":R#d_next_o_id:1:I";
					// Utilities.getEntityLogString(TPCCConstants.ORDER_ENTITY, orderId, orderPropertiesString);
					// String logDistrict = "";// getEntityLogString(TPCCConstants.DISTRICT_ENTITY, districtId, districtPropertiesString);
					// String logStocks = ""; // getStocksLogString(ol_count);

					String UpdateLogString = Utilities.getLogString(ValidationConstants.READ_WRITE_RECORD, TPCCConstants.NEWORDER_ACTION, threadId, sequenceId, startTime, endTime, logCustOrdRelationship, logOrder, logDist/* , logDistrict, logStocks */);
					this.updateLog.append(UpdateLogString);

				}

				else {
					// System.out.println("##Updating state New Order Action...");
					Order newOrder = new Order();
					// customer = ValidationMain.customer.get(customerId);

					Entity customerE = null;// ValidationMain.dbState.get(TPCCConstants.CUSTOMER_ENTITY).get(customerId).get(0);
					String old_oid = customerE.getProperties()[3].getValue();

					customerE.getProperties()[3].setValue(String.valueOf(o_id));

					Property o_ol_cntP = new Property(TPCCConstants.ORDER_PROPERIES[0], String.valueOf(ol_count), '.');
					Property o_carrier_idP = new Property(TPCCConstants.ORDER_PROPERIES[0], String.valueOf(o_carrier_id), '.');
					Property ol_delivery_dP = new Property(TPCCConstants.ORDER_PROPERIES[0], String.valueOf(ol_delivery_d), '.');
					Property[] o_properties = { o_ol_cntP, o_carrier_idP, ol_delivery_dP };

					Entity orderE = new Entity(orderId, TPCCConstants.ORDER_ENTITY, o_properties);
					ArrayList<Entity> arrayListOrder = new ArrayList<Entity>();
					arrayListOrder.add(orderE);
					// ValidationMain.dbState.get(TPCCConstants.ORDER_ENTITY).put(orderId, arrayListOrder);

					// customerE.getProperties()[3].setValue(String.valueOf(o_carrier_id));
					// customerE.getProperties()[3].setValue(String.valueOf(ol_count));
					// newOrder.order_carrier_id = o_carrier_id;
					// newOrder.order_ol_count = (int) ol_count;
					// long orderlinesD[] = new long[(int) ol_count];
					//
					// for (int i = 0; i < ol_count; i++) {
					// orderlinesD[i] = ol_delivery_d;
					// }
					// newOrder.orderLines_delivery_d = orderlinesD;
					// districtInfo.put(districtId, o_id + 1);
					// ValidationMain.customers.put(customerId, customer);
					// orderInfo.put(orderId, newOrder);
					// System.out.println("Last order for customer:" + customerId + " is:" + o_id);
				}
				break;
			case 'P':
				//
				// actionName = "Payment";
				w_id = (Integer) this.transactionResults.get("w_id");
				d_id = (Integer) this.transactionResults.get("d_id");
				c_id = (Integer) this.transactionResults.get("c_id");
				String c_bal = String.valueOf(( transactionResults.get("c_balance")));
				// String c_bal = (String) transactionResults.get("c_balance");
				// c_bal=TPCCConstants.DECIMAL_FORMAT.format(c_bal);
				float ytd_payment = (Float) transactionResults.get("c_ytd_payment");
				// ytd_payment = Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(ytd_payment));
				int pcnt = (Integer) transactionResults.get("c_payment_cnt");

				String readBal = (String) transactionResults.get("readBalance");
				// String readBal = (String) transactionResults.get("readBalance");
				float readYTD_payment = (Float) transactionResults.get("readYTD_payment");
				// readYTD_payment = Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(readYTD_payment));
				int readPCNT = (Integer) transactionResults.get("readPayment_cnt");
				customerId = generateID(w_id, d_id, c_id);

				if (TPCCConstants.FILE_LOG) {

					// String customerPropertiesString = TPCCConstants.CUSTOMER_ENTITY + TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + customerId + TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + TPCCConstants.CUSTOMER_PROPERIES[0] + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + c_bal + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + TPCCConstants.NEW_VALUE_UPDATE + TPCCConstants.PROPERY_SEPERATOR + TPCCConstants.CUSTOMER_PROPERIES[1] + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + ytd_payment
					// + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + TPCCConstants.NEW_VALUE_UPDATE + TPCCConstants.PROPERY_SEPERATOR + TPCCConstants.CUSTOMER_PROPERIES[2] + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + pcnt + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + TPCCConstants.NEW_VALUE_UPDATE;
					String logCustomer = get_Payment_Customer_String(customerId, c_bal, ytd_payment, pcnt, readBal, readYTD_payment, readPCNT);

					String UpdateLogString = Utilities.getLogString(ValidationConstants.READ_WRITE_RECORD, TPCCConstants.PAYMENT_ACTION, threadId, sequenceId, startTime, endTime, logCustomer);
					this.updateLog.append(UpdateLogString);

				}
				// else {
				// System.out.println("##Updating state Payment Action...");
				// customer = ValidationMain.customers.get(customerId);
				// if (customer == null) {
				// System.out.println("Customer is null:" + customerId);
				// System.exit(0);
				// }
				//
				// if (this.transactionResults.get("c_data") != null) {
				// customer.c_data = (String)
				// this.transactionResults.get("c_data");
				//
				// }
				//
				// c_bal = (String) transactionResults.get("c_balance");
				// customer.c_balance =
				// Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(c_bal));
				// ytd_payment = (Float)
				// transactionResults.get("c_ytd_payment");
				// customer.c_ytd_payment =
				// Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(ytd_payment));
				// customer.c_payment_cnt = (Integer)
				// transactionResults.get("c_payment_cnt");
				//
				// }
				break;
			case 'D':
				// actionName = "Delivery";
				//

				w_id = (Integer) this.transactionResults.get("w_id");
				o_carrier_id = (Integer) this.transactionResults.get("o_carrier_id");
				long deliveryDate = (Long) this.transactionResults.get("delivery_date");
				deliveryDate = deliveryDate / TPCCConstants.DELIVERY_DATE_DIVISION;
				ArrayList<String> customers = new ArrayList<String>();
				ArrayList<String> orders = new ArrayList<String>();
				for (d_id = 1; d_id <= TPCCConfig.configDistPerWhse; d_id++) {

					o_id = (Integer) transactionResults.get("o" + d_id);

					if (o_id == -1) {
						continue;
					}
					c_id = (Integer) transactionResults.get("c" + d_id);
					// c_bal = String.valueOf((Float) transactionResults.get("b" + d_id));
					String c_balString = String.valueOf(transactionResults.get("b" + d_id));
					c_bal = c_balString;
					orderId = generateID(w_id, d_id, c_id, o_id);
					customerId = generateID(w_id, d_id, c_id);

					//
					// c_bal = String.valueOf(TPCCConstants.DECIMAL_FORMAT.format(Double.parseDouble(c_bal)));
					if (TPCCConstants.FILE_LOG) {
						// String customerPropertiesString = TPCCConstants.CUSTOMER_ENTITY + TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + customerId + TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + TPCCConstants.CUSTOMER_PROPERIES[0] + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + c_bal + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + TPCCConstants.INCREMENT_UPDATE;
						//
						// String orderPropertiesString = TPCCConstants.ORDER_ENTITY + TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + orderId + TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + TPCCConstants.ORDER_PROPERIES[1] + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + o_carrier_id + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + TPCCConstants.NEW_VALUE_UPDATE + TPCCConstants.PROPERY_SEPERATOR + TPCCConstants.ORDER_PROPERIES[2] + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + deliveryDate
						// + TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + TPCCConstants.NEW_VALUE_UPDATE;

						String logCustomer = get_Delivery_Customer_String(customerId, c_bal);
						String logOrder = get_Delivery_Order_String(orderId, o_carrier_id, deliveryDate);// Utilities.getEntityLogString(TPCCConstants.ORDER_ENTITY, orderId, orderPropertiesString);

						customers.add(logCustomer);
						orders.add(logOrder);

					}
					// else {
					// Customer cust = ValidationMain.customers.get(customerId);
					// cust.c_balance =
					// Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(cust.c_balance))
					// + Float.parseFloat(c_balString);
					// Order order = orderInfo.get(orderId);
					// if (order == null) {
					// System.out.println("wid:" + w_id + " did:" + d_id + "
					// oid:" + o_id + " order key=" + orderId);
					// }
					// order.order_carrier_id = o_carrier_id;
					// for (int j = 0; j < order.orderLines_delivery_d.length;
					// j++) {
					// order.orderLines_delivery_d[j] = deliveryDate;
					// }
					//
					// }
					//
				}

				String UpdateLogString = Utilities.getLogString(TPCCConstants.UPDATE_RECORD, TPCCConstants.DELIVERY_ACTION, threadId, sequenceId, startTime, endTime, customers, orders);
				this.updateLog.append(UpdateLogString);

				break;
			case 'S':
			    w_id = (int)this.transactionResults.get("w_id");
			    d_id = (int)this.transactionResults.get("d_id");
			    int T = (int) this.transactionResults.get("T");
			    o_id = (int)this.transactionResults.get("d_next_o_id");
			    String itemIds = (String)this.transactionResults.get("item_ids");
			    int stkCount = (int)this.transactionResults.get("stock_count");
			    String str = String.format("w_id:%d;d_id:%d;T:%d;d_next_o_id:%d;item_ids:%s;stock_count:%d\n", w_id, d_id, T, o_id, itemIds, stkCount);
			    this.stkLog.append(str);
			default:
				// System.out.println("Transaction name is invalid:" + transName);
			}

			this.transactionResults.clear();
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
			System.exit(0);
		}
		try {
			if (TPCCConstants.FILE_LOG) {
				if (readLog != null) {
					readLogAll.write(readLog.toString());

					readLog.delete(0, readLog.length());
				}
				if (updateLog != null) {

					updateLogAll.write(updateLog.toString());

					updateLog.delete(0, updateLog.length());
				}
				
				if (stkLog != null) {
				    stkLogAll.write(stkLog.toString());
				    stkLog.delete(0, stkLog.length());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
	}
	

	private String get_Delivery_Order_String(String orderId, int o_carrier_id, long ol_delivery_d) {
		String[] OrderProperties = new String[TPCCConstants.ORDER_PROPERIES.length];
		char[] OrderPropertiesType = new char[TPCCConstants.ORDER_PROPERIES.length];
		for (int i = 0; i < TPCCConstants.ORDER_PROPERIES.length; i++) {
			if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_ORDER_COUNT)) {
				OrderProperties[i] = null;
				OrderPropertiesType[i] = TPCCConstants.NO_READ_UPDATE;
			} else if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_CARRIER_ID)) {
				OrderProperties[i] = String.valueOf(o_carrier_id);
				OrderPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_DELIVERY_DATE)) {
				OrderProperties[i] = String.valueOf(ol_delivery_d);
				OrderPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.ORDER_PROPERIES[i], TPCCConstants.ORDER_ENTITY);
				System.exit(0);
			}
		}
		String OrderPropertiesString = Utilities.getPropertiesString2(TPCCConstants.ORDER_PROPERIES, OrderProperties, OrderPropertiesType);
		String OrderKey = orderId;
		return Utilities.getEntityLogString(TPCCConstants.ORDER_ENTITY, OrderKey, OrderPropertiesString);
	}

	private String get_Delivery_Customer_String(String customerId, String c_balance) {
		String[] CustomerProperties = new String[TPCCConstants.CUSTOMER_PROPERIES.length];
		char[] CustomerPropertiesType = new char[TPCCConstants.CUSTOMER_PROPERIES.length];
		for (int i = 0; i < TPCCConstants.CUSTOMER_PROPERIES.length; i++) {
			if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_BALANCE)) {
				CustomerProperties[i] = c_balance;
				CustomerPropertiesType[i] = TPCCConstants.INCREMENT_UPDATE;
			} else if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_YTD_PAYMENT)) {
				CustomerProperties[i] = null;
				CustomerPropertiesType[i] = TPCCConstants.NO_READ_UPDATE;
			} else if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_PAYMENT_COUNT)) {
				CustomerProperties[i] = null;
				CustomerPropertiesType[i] = TPCCConstants.NO_READ_UPDATE;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.CUSTOMER_PROPERIES[i], TPCCConstants.CUSTOMER_ENTITY);
				System.exit(0);
			}
		}
		String CustomerPropertiesString = Utilities.getPropertiesString2(TPCCConstants.CUSTOMER_PROPERIES, CustomerProperties, CustomerPropertiesType);
		String CustomerKey = customerId;
		return Utilities.getEntityLogString(TPCCConstants.CUSTOMER_ENTITY, CustomerKey, CustomerPropertiesString);
	}

	private String get_Payment_Customer_String(String customerId, String c_balance, float c_ytd_payment, int c_payment_cnt, String readBalance, float readYTD_payment, int readPayment_cnt) {
		String[] CustomerProperties = new String[TPCCConstants.CUSTOMER_PROPERIES.length * 2];
		char[] CustomerPropertiesType = new char[TPCCConstants.CUSTOMER_PROPERIES.length * 2];
		String[] CustomerPropertiesNames = new String[TPCCConstants.CUSTOMER_PROPERIES.length * 2];
		for (int i = 0; i < TPCCConstants.CUSTOMER_PROPERIES.length; i++) {
			CustomerPropertiesNames[i] = TPCCConstants.CUSTOMER_PROPERIES[i];
			if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_BALANCE)) {
				CustomerProperties[i] = c_balance;
				CustomerPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_YTD_PAYMENT)) {
				CustomerProperties[i] = String.valueOf(c_ytd_payment);
				CustomerPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_PAYMENT_COUNT)) {
				CustomerProperties[i] = String.valueOf(c_payment_cnt);
				CustomerPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.CUSTOMER_PROPERIES[i], TPCCConstants.CUSTOMER_ENTITY);
				System.exit(0);
			}
		}
		for (int i = 0; i < TPCCConstants.CUSTOMER_PROPERIES.length; i++) {
			CustomerPropertiesNames[i + TPCCConstants.CUSTOMER_PROPERIES.length] = TPCCConstants.CUSTOMER_PROPERIES[i];
			if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_BALANCE)) {
				CustomerProperties[i + TPCCConstants.CUSTOMER_PROPERIES.length] = readBalance;
				CustomerPropertiesType[i + TPCCConstants.CUSTOMER_PROPERIES.length] = TPCCConstants.READ_RECORD;
			} else if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_YTD_PAYMENT)) {
				CustomerProperties[i + TPCCConstants.CUSTOMER_PROPERIES.length] = String.valueOf(readYTD_payment);
				CustomerPropertiesType[i + TPCCConstants.CUSTOMER_PROPERIES.length] = TPCCConstants.READ_RECORD;
			} else if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_PAYMENT_COUNT)) {
				CustomerProperties[i + TPCCConstants.CUSTOMER_PROPERIES.length] = String.valueOf(readPayment_cnt);
				CustomerPropertiesType[i + TPCCConstants.CUSTOMER_PROPERIES.length] = TPCCConstants.READ_RECORD;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.CUSTOMER_PROPERIES[i], TPCCConstants.CUSTOMER_ENTITY);
				System.exit(0);
			}
		}
		String CustomerPropertiesString = Utilities.getPropertiesString2(CustomerPropertiesNames, CustomerProperties, CustomerPropertiesType);
		String CustomerKey = customerId;
		return Utilities.getEntityLogString(TPCCConstants.CUSTOMER_ENTITY, CustomerKey, CustomerPropertiesString);
	}

	private String get_OrderStatus_Customer_String(String customerId, float c_balance, float c_ytd_payment, int c_payment_cnt) {
		String[] CustomerProperties = new String[TPCCConstants.CUSTOMER_PROPERIES.length];
		char[] CustomerPropertiesType = new char[TPCCConstants.CUSTOMER_PROPERIES.length];
		for (int i = 0; i < TPCCConstants.CUSTOMER_PROPERIES.length; i++) {
			if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_BALANCE)) {
				CustomerProperties[i] = String.valueOf(c_balance);
				CustomerPropertiesType[i] = TPCCConstants.READ_RECORD;
			} else if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_YTD_PAYMENT)) {
				CustomerProperties[i] = String.valueOf(c_ytd_payment);
				CustomerPropertiesType[i] = TPCCConstants.READ_RECORD;
			} else if (TPCCConstants.CUSTOMER_PROPERIES[i].equals(TPCCConstants.CUSTOMER_PAYMENT_COUNT)) {
				CustomerProperties[i] = String.valueOf(c_payment_cnt);
				CustomerPropertiesType[i] = TPCCConstants.READ_RECORD;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.CUSTOMER_PROPERIES[i], TPCCConstants.CUSTOMER_ENTITY);
				System.exit(0);
			}
		}
		String CustomerPropertiesString = Utilities.getPropertiesString2(TPCCConstants.CUSTOMER_PROPERIES, CustomerProperties, CustomerPropertiesType);
		String CustomerKey = customerId;
		return Utilities.getEntityLogString(TPCCConstants.CUSTOMER_ENTITY, CustomerKey, CustomerPropertiesString);
	}

	private String get_NewOrder_Order_String(String orderId, int ol_count, int o_carrier_id, long ol_delivery_d) {
		String[] OrderProperties = new String[TPCCConstants.ORDER_PROPERIES.length];
		char[] OrderPropertiesType = new char[TPCCConstants.ORDER_PROPERIES.length];
		for (int i = 0; i < TPCCConstants.ORDER_PROPERIES.length; i++) {
			if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_ORDER_COUNT)) {
				OrderProperties[i] = String.valueOf(ol_count);
				OrderPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_CARRIER_ID)) {
				OrderProperties[i] = String.valueOf(o_carrier_id);
				OrderPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_DELIVERY_DATE)) {
				OrderProperties[i] = String.valueOf(ol_delivery_d);
				OrderPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.ORDER_PROPERIES[i], TPCCConstants.ORDER_ENTITY);
				System.exit(0);
			}
		}
		String OrderPropertiesString = Utilities.getPropertiesString2(TPCCConstants.ORDER_PROPERIES, OrderProperties, OrderPropertiesType);
		String OrderKey = orderId;
		return Utilities.getEntityLogString(TPCCConstants.ORDER_ENTITY, OrderKey, OrderPropertiesString);
	}

	private String get_OrderStatus_Order_String(String orderId, int ol_count, int o_carrier_id, long ol_delivery_d) {
		String[] OrderProperties = new String[TPCCConstants.ORDER_PROPERIES.length];
		char[] OrderPropertiesType = new char[TPCCConstants.ORDER_PROPERIES.length];
		for (int i = 0; i < TPCCConstants.ORDER_PROPERIES.length; i++) {
			if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_ORDER_COUNT)) {
				OrderProperties[i] = String.valueOf(ol_count);
				OrderPropertiesType[i] = TPCCConstants.READ_RECORD;
			} else if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_CARRIER_ID)) {
				OrderProperties[i] = String.valueOf(o_carrier_id);
				OrderPropertiesType[i] = TPCCConstants.READ_RECORD;
			} else if (TPCCConstants.ORDER_PROPERIES[i].equals(TPCCConstants.ORDER_DELIVERY_DATE)) {
				OrderProperties[i] = String.valueOf(ol_delivery_d);
				OrderPropertiesType[i] = TPCCConstants.READ_RECORD;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.ORDER_PROPERIES[i], TPCCConstants.ORDER_ENTITY);
				System.exit(0);
			}
		}
		String OrderPropertiesString = Utilities.getPropertiesString2(TPCCConstants.ORDER_PROPERIES, OrderProperties, OrderPropertiesType);
		String OrderKey = orderId;
		return Utilities.getEntityLogString(TPCCConstants.ORDER_ENTITY, OrderKey, OrderPropertiesString);
	}

	private String get_OrderStatus_CustOrdRelationship_String(String relatioshipId, String customerId, String orderId) {
		String[] cust_ord_RelationshipProperties = new String[TPCCConstants.CUST_ORDER_REL_PROPERIES.length];
		char[] cust_ord_RelationshipPropertiesType = new char[TPCCConstants.CUST_ORDER_REL_PROPERIES.length];
		for (int i = 0; i < TPCCConstants.CUST_ORDER_REL_PROPERIES.length; i++) {
			if (TPCCConstants.CUST_ORDER_REL_PROPERIES[i].equals(TPCCConstants.CUSTOMER_ENTITY)) {
				cust_ord_RelationshipProperties[i] = Utilities.concat(TPCCConstants.CUSTOMER_ENTITY, TPCCConstants.KEY_SEPERATOR, customerId);
				cust_ord_RelationshipPropertiesType[i] = TPCCConstants.READ_RECORD;
			} else if (TPCCConstants.CUST_ORDER_REL_PROPERIES[i].equals(TPCCConstants.ORDER_ENTITY)) {
				cust_ord_RelationshipProperties[i] = Utilities.concat(TPCCConstants.ORDER_ENTITY, TPCCConstants.KEY_SEPERATOR, orderId);
				cust_ord_RelationshipPropertiesType[i] = TPCCConstants.READ_RECORD;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.CUST_ORDER_REL_PROPERIES[i], TPCCConstants.CUST_ORDER_REL);
				System.exit(0);
			}
		}
		String cust_ord_RelationshipProprtiesString = Utilities.getPropertiesString2(TPCCConstants.CUST_ORDER_REL_PROPERIES, cust_ord_RelationshipProperties, cust_ord_RelationshipPropertiesType);
		String cust_ord_RelationshipKey = relatioshipId;
		return Utilities.getEntityLogString(TPCCConstants.CUST_ORDER_REL, cust_ord_RelationshipKey, cust_ord_RelationshipProprtiesString);
	}

	private String get_NewOrder_CustOrdRelationship_String(String relatioshipId, String customerId, String orderId) {
		String[] cust_ord_RelationshipProperties = new String[TPCCConstants.CUST_ORDER_REL_PROPERIES.length];
		char[] cust_ord_RelationshipPropertiesType = new char[TPCCConstants.CUST_ORDER_REL_PROPERIES.length];
		for (int i = 0; i < TPCCConstants.CUST_ORDER_REL_PROPERIES.length; i++) {
			if (TPCCConstants.CUST_ORDER_REL_PROPERIES[i].equals(TPCCConstants.CUSTOMER_ENTITY)) {
				cust_ord_RelationshipProperties[i] = Utilities.concat(TPCCConstants.CUSTOMER_ENTITY, TPCCConstants.KEY_SEPERATOR, customerId);
				cust_ord_RelationshipPropertiesType[i] = TPCCConstants.NO_READ_UPDATE;
			} else if (TPCCConstants.CUST_ORDER_REL_PROPERIES[i].equals(TPCCConstants.ORDER_ENTITY)) {
				cust_ord_RelationshipProperties[i] = Utilities.concat(TPCCConstants.ORDER_ENTITY, TPCCConstants.KEY_SEPERATOR, orderId);
				cust_ord_RelationshipPropertiesType[i] = TPCCConstants.NEW_VALUE_UPDATE;
			} else {
				System.out.printf("ERROR: Unhandled proprty \"%s\" in Entity \"%s\".\n", TPCCConstants.CUST_ORDER_REL_PROPERIES[i], TPCCConstants.CUST_ORDER_REL);
				System.exit(0);
			}
		}
		String cust_ord_RelationshipProprtiesString = Utilities.getPropertiesString2(TPCCConstants.CUST_ORDER_REL_PROPERIES, cust_ord_RelationshipProperties, cust_ord_RelationshipPropertiesType);
		String cust_ord_RelationshipKey = relatioshipId;
		return Utilities.getEntityLogString(TPCCConstants.CUST_ORDER_REL, cust_ord_RelationshipKey, cust_ord_RelationshipProprtiesString);
	}

	private void printStale(long ev, long rv, String customerId, String operationId, String actionType) {
		// TODO Auto-generated method stub
		System.out.println("D Stale Value Read:" + rv + " Value expected:" + ev + " For operation:" + operationId + " Action:" + actionType);
		System.exit(0);

	}

	public static String generateID(int... params) {
		// TODO Auto-generated method stub
		String result = "";// String.valueOf(k);
		for (int i : params) {
			result += "-" + i;
		}
		result = result.substring(1);
		return result;

	}

	private void printStale(Double expectedV, double observedV, String itemId, String operationId, String actionType) {
		// TODO Auto-generated method stub
		System.out.println("Stale Value Read:" + observedV + " Value expected:" + expectedV + " For operation:" + operationId + " Action:" + actionType);
		try {
			if (TPCCConstants.FILE_LOG) {
				readLogAll.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.exit(0);
	}
}
