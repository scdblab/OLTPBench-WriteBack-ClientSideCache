package com.oltpbenchmark.benchmarks.tpcc.validation;

import java.util.ArrayList;
import java.util.LinkedList;

public class IntervalsList {
	LinkedList<LogRecord> intervals;

	IntervalsList() {
		intervals = new LinkedList<LogRecord>();
	}

	public void insert(LogRecord record) {
		intervals.add(record);
	}

	public void remove(LogRecord record) {
		intervals.remove(record);
	}

	public ArrayList<LogRecord> getAll(long start, long end) {
		LogRecord X = new LogRecord(null, null, 0, end, 'X', null);// TODO replaced start with 0
		ArrayList<LogRecord> result = new ArrayList<LogRecord>();
		for (int i = 0; i < intervals.size(); i++) {
			LogRecord r = intervals.get(i);
			// if (X.getEndTime() >= r.getStartTime())
			if (X.overlap(r))
				result.add(r);
			else
				return result;
		}
		return result;
	}

	public boolean contains(String id) {
		for (int i = 0; i < intervals.size(); i++) {
			LogRecord r = intervals.get(i);
			if (r.getId().equals(id))
				return true;
		}
		return false;
	}

	public int size() {
		return intervals.size();
	}

	public void addIntervalToStart(LogRecord log) {
		intervals.addFirst(log);
	}

	public void addIntervalSorted(LogRecord log) {
		if (intervals.isEmpty()) {

			intervals.add(log);
		} else {
			for (int i = 0; i < intervals.size(); i++) {
				if (intervals.get(i).getStartTime() >= log.getStartTime()) {
					intervals.add(i, log);
					return;
				}
			}
			intervals.add(log);

		}
	}

	public int debug_searchFor(String id) {
		for (int i = 0; i < intervals.size(); i++) {
			if (intervals.get(i).getId().equals(id))
				return i;
		}
		return -1;
	}

	public void clear() {
		intervals.clear();
	}

}
