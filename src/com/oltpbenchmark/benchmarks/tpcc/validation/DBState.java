package com.oltpbenchmark.benchmarks.tpcc.validation;


//import java.util.ArrayList;

public class DBState {
	// private Object[] value;
	protected String[] value;
	protected int refrenceCount;
	// private ArrayList<DBState> referencingMe;

	public DBState(int count, String... params) {
		value = params;
		refrenceCount = count;
		// addReferencingMe(null);
	}

	public String[] getValue() {
		return value;
	}

	public void setValue(String[] value) {
		this.value = value;
	}

	public int getRefrenceCount() {
		return refrenceCount;
	}

	public void setRefrenceCount(int refrenceCount) {
		this.refrenceCount = refrenceCount;
	}

	public void increment(/* String key */) {
		// if (ValidationConstants.debug && key.equals("MEMBER-165"))
		// System.out.println("increment MEMBER-165");
		refrenceCount++;
	}

	public void decrement(/* String key */) {
		// if (ValidationConstants.debug && key.equals("MEMBER-165"))
		// System.out.println("decrement MEMBER-165");
		refrenceCount--;
	}

	public int size() {
		return value.length;
	}

	public boolean same(DBState newState) {
		for (int i = 0; i < value.length; i++) {
			if (!ValidationConstants.hasInitState){
			if ((value[i]==null) && (newState.value[i]==null))
				continue;
		if (value[i]==null ){
			value[i]=newState.value[i];
		}
			}
			if (!value[i].equals(newState.value[i]))
				return false;
			// }
		}
		return true;
	}

	@Override
	public String toString() {
		// String test = String.valueOf(System.identityHashCode(this));
		String result = "[";
		for (int i = 0; i < value.length; i++) {
			result += value[i];
			if (i + 1 != value.length)
				result += ", ";
		}
		result += "]";
		return result;
	}

	// public ArrayList<DBState> getReferencingMe() {
	// return referencingMe;
	// }
	//
	// public void addReferencingMe(DBState state) {
	// if(this.referencingMe == null){
	// this.referencingMe = new ArrayList<DBState>();
	// }
	// this.referencingMe.add(state);
	// }
}
