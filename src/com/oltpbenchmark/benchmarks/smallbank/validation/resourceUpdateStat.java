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

package com.oltpbenchmark.benchmarks.smallbank.validation;

import java.util.ArrayList;

/**
 * keeps a track of the first update details for a resource and its related interval if applicable constructs the appropriate interval tree or hashmap
 *
 * @author barahman
 *
 */
public class resourceUpdateStat {
	// the min time when an update started on this resource
	// the max time when an update ended on this resource
	// the final value expected for this resource
	// timeIntTree is used when the intervaltree approach is used for validation

	long minStartTime = 0, maxEndTime = 0, maxStartTime = 0;

	// boolean isFinalValCalc=false;

	public long getMaxStartTime() {
		return maxStartTime;
	}

	public void setMaxStartTime(long maxStartTime) {
		this.maxStartTime = maxStartTime;
	}

	IntervalsList timeIntTree = new IntervalsList(); // there should be only one instance of this for every resource
	private long latestKafkaWriteOffset;

	public long getLatestKafkaWriteOffset() {
		return latestKafkaWriteOffset;
	}

	public void setLatestKafkaWriteOffset(long latestKafkaWriteOffset) {
		this.latestKafkaWriteOffset = latestKafkaWriteOffset;
	}

	resourceUpdateStat() {

	}

	public synchronized void addInterval(LogRecord record) {
		if (record.getStartTime()>maxStartTime)
		maxStartTime = record.getStartTime();
		timeIntTree.insert(record);
		// List<Interval<Long>> its = timeIntTree.getIntervals(start, end);
		// System.out.println(((Interval<Long>)(its.get(0))).getData()+" "+((Interval<Long>)(its.get(0))).getStart()+" "+((Interval<Long>)(its.get(0))).getEnd());
		/*
		 * List<Long> its2 = timeIntTree.get(start, end); System.out.println("**"+((Long)(its2.get(0))).longValue());
		 */
	}

	public synchronized void removeInterval(LogRecord record) {
		timeIntTree.remove(record);
	}

	public ArrayList<LogRecord> queryIntervalTree(long start, long end) {
		return timeIntTree.getAll(start, end);
	}

	public long getMinStartTime() {
		return minStartTime;
	}

	public void setMinStartTime(long minStartTime) {
		this.minStartTime = minStartTime;
	}

	public long getMaxEndTime() {
		return maxEndTime;
	}

	public void setMaxEndTime(long maxEndTime) {
		this.maxEndTime = maxEndTime;
	}

	public void deleteIntervalsBefore(long et) {
		// List<Interval<LogRecord>> n = queryIntervalTree(0, et);
		// Iterator<Interval<LogRecord>> it = n.iterator();
		// while (it.hasNext()) {
		// Interval<LogRecord> i = it.next();
		// if (i.getEnd() < et)
		// it.remove();
		// }
	}

	public boolean contains(String id) {
		return this.timeIntTree.contains(id);
	}

	public int size() {
		return timeIntTree.size();
	}

	public void addIntervalToStart(LogRecord log) {
		if (log.getStartTime()>maxStartTime)
			maxStartTime = log.getStartTime();
		
		timeIntTree.addIntervalToStart(log);
	}

	public void addIntervalSorted(LogRecord log) {
		if (log.getStartTime()>maxStartTime)
			maxStartTime = log.getStartTime();

		timeIntTree.addIntervalSorted(log);
	}

	public int debug_searchFor(String id) {
		return timeIntTree.debug_searchFor(id);
	}

	public void addIntervalSorted(LogRecord log, long kafkaOffset) {
		if (log.getStartTime()>maxStartTime)
			maxStartTime = log.getStartTime();

		timeIntTree.addIntervalSorted(log);
		
		latestKafkaWriteOffset = kafkaOffset;
		
	}

	public void clear() {
		timeIntTree.clear();
		
	}

}
