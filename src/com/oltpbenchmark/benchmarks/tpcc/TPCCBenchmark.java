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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;

public class TPCCBenchmark extends BenchmarkModule {
	private static final Logger LOG = Logger.getLogger(TPCCBenchmark.class);

	private SockIOPool cacheConnectionPool;

	public TPCCBenchmark(WorkloadConfiguration workConf) {
		super("tpcc", workConf, true);

		if (Config.CAFE) {
			cacheConnectionPool = SockIOPool.getInstance(TPCCConstants.CACHE_POOL_NAME);
			cacheConnectionPool.setServers(Config.cacheServers);
//			cacheConnectionPool.setFailover(false);
			cacheConnectionPool.setInitConn(5);
			cacheConnectionPool.setMinConn(5);
			cacheConnectionPool.setMaxConn(200);
			cacheConnectionPool.setMaintSleep(0);
			cacheConnectionPool.setNagle(true);
//			cacheConnectionPool.setSocketTO(0);
//			cacheConnectionPool.setAliveCheck(false);
			cacheConnectionPool.initialize();
			System.out.println("Cache servers: "+Arrays.toString(Config.cacheServers));
		}
	}

	@Override
	protected Package getProcedurePackageImpl() {
		return (NewOrder.class.getPackage());
	}

	/**
	 * @param Bool
	 */
	@Override
	protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
		ArrayList<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();

		try {
			List<TPCCWorker> terminals = createTerminals();
			workers.addAll(terminals);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return workers;
	}

	@Override
	protected Loader<TPCCBenchmark> makeLoaderImpl(Connection conn) throws SQLException {
		return new TPCCLoader(this, conn);
	}

	protected ArrayList<TPCCWorker> createTerminals() throws SQLException {
		TPCCWorker[] terminals = new TPCCWorker[workConf.getTerminals()];

		int minWarehouseId = (int) workConf.getMinWarehouseId();
		int maxWarehouseId = (int) workConf.getMaxWarehouseId();
		assert maxWarehouseId - minWarehouseId >= 0;
		int numWarehouses = maxWarehouseId - minWarehouseId + 1;
//		int numWarehouses = (int) workConf.getScaleFactor();//tpccConf.getNumWarehouses();
//		if (numWarehouses <= 0) {
//			numWarehouses = 1;
//		}
		int numTerminals = workConf.getTerminals();
		assert (numTerminals >= numWarehouses) :
			String.format("Insufficient number of terminals '%d' [numWarehouses=%d]",
					numTerminals, numWarehouses);

		// TODO: This is currently broken: fix it!
//		int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
//		assert warehouseOffset == 1;

		// We distribute terminals evenly across the warehouses
		// Eg. if there are 10 terminals across 7 warehouses, they
		// are distributed as
		// 1, 1, 2, 1, 2, 1, 2
		final double terminalsPerWarehouse = (double) numTerminals
				/ numWarehouses;
		int workerId = 0;
		assert terminalsPerWarehouse >= 1;
//		for (int w = 0; w < numWarehouses; w++) {
		for (int w = minWarehouseId; w <= maxWarehouseId; w++) {
			// Compute the number of terminals in *this* warehouse
			int lowerTerminalId = (int) ((w-minWarehouseId) * terminalsPerWarehouse);
			int upperTerminalId = (int) ((w-minWarehouseId + 1) * terminalsPerWarehouse);
			// protect against double rounding errors
//			int w_id = w + 1;
			int w_id = w;
//			if (w_id == numWarehouses)
//				upperTerminalId = numTerminals;
//          if (w_id == maxWarehouseId)
//              upperTerminalId = maxWarehouseId+1;
			int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

			if (LOG.isDebugEnabled())
				LOG.debug(String.format("w_id %d = %d terminals [lower=%d / upper%d]",
						w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

			final double districtsPerTerminal = TPCCConfig.configDistPerWhse
					/ (double) numWarehouseTerminals;
			assert districtsPerTerminal >= 1 :
				String.format("Too many terminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]",
						districtsPerTerminal, numWarehouseTerminals);
			for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
				int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
				int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
				if (terminalId + 1 == numWarehouseTerminals) {
					upperDistrictId = TPCCConfig.configDistPerWhse;
				}
				lowerDistrictId += 1;

				TPCCWorker terminal = new TPCCWorker(this, workerId++,
						w_id, lowerDistrictId, upperDistrictId,
						numWarehouses);
				terminals[lowerTerminalId + terminalId] = terminal;
			}

		}
		assert terminals[terminals.length - 1] != null;

		ArrayList<TPCCWorker> ret = new ArrayList<TPCCWorker>();
		for (TPCCWorker w : terminals)
			ret.add(w);
		return ret;
	}

	/**
	 * Hack to support postgres-specific timestamps
	 * @param time
	 * @return
	 */
	public Timestamp getTimestamp(long time) {
		Timestamp timestamp;
		timestamp = new java.sql.Timestamp(time);
		return (timestamp);
	}
}