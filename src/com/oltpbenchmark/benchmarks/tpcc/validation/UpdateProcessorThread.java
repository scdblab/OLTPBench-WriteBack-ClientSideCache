/**
 * Copyright (c) 2012 USC Database Laboratory All rights reserved.
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.oltpbenchmark.benchmarks.tpcc.validation;

import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class UpdateProcessorThread extends Thread {
	Semaphore _semaphore;
	Properties _props;
	ConcurrentHashMap<String, resourceUpdateStat> _updateStats;
	Vector<LogRecord> _updatesToBeProcessed;
	Semaphore _putSemaphore;

	UpdateProcessorThread(ConcurrentHashMap<String, resourceUpdateStat> updateStats, Vector<LogRecord> updatesToBeProcessed, Semaphore semaphore, Semaphore putSemaphore) {
		_semaphore = semaphore;
		// _props = props;
		_updateStats = updateStats;
		_updatesToBeProcessed = updatesToBeProcessed;
		_putSemaphore = putSemaphore;
	}

	@Override
	public void run() {

		try {
			_semaphore.acquire();

			Iterator<LogRecord> it = _updatesToBeProcessed.iterator();
			while (it.hasNext()) {
				LogRecord record = (it.next());
				// TODO remove the LogRecord form the vector
				updateResource(record);
				_updateStats.get(record.getPartitionID()).addInterval(record);

			}
			_semaphore.release();
		} catch (InterruptedException e1) {
			e1.printStackTrace(System.out);
		}
	}

	/**
	 * @param record
	 * @throws InterruptedException
	 *             check if an update has been seen for this resource before if so then update the available structures
	 */
	private void updateResource(LogRecord record) throws InterruptedException {
		_putSemaphore.acquire();
		resourceUpdateStat newVal = _updateStats.get(record.getPartitionID());
		if (newVal == null) {// record doesn't exist
			newVal = new resourceUpdateStat();
			newVal.setMinStartTime(record.getStartTime());
			newVal.setMaxEndTime(record.getEndTime());
			newVal.setMaxStartTime(record.getStartTime());
			_updateStats.put(record.getPartitionID(), newVal);
		} else {
			long tempValMinS = newVal.getMinStartTime();
			long tempValMaxS = newVal.getMaxStartTime();
			long tempValMaxE = newVal.getMaxEndTime();
			// String tempValV = newVal.getFinalVal();

			// update min start time if needed
			if (tempValMinS > record.getStartTime()) {
				newVal.setMinStartTime(record.getStartTime());
			}
			// update max end time if needed
			if (tempValMaxE < record.getEndTime()) {
				newVal.setMaxEndTime(record.getEndTime());
			}
			// update max start time if needed
			if (tempValMaxS < record.getStartTime()) {
				newVal.setMaxStartTime(record.getStartTime());
			}
		}
		_putSemaphore.release();
	}
}