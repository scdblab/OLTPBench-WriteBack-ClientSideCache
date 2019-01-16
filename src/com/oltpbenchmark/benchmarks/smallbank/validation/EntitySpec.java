package com.oltpbenchmark.benchmarks.smallbank.validation;

import java.util.ArrayList;

public class EntitySpec extends Entity {
	ArrayList <Property> propertiesArrayLis=null;

	public ArrayList<Property> getPropertiesArrayLis() {
		return propertiesArrayLis;
	}

	public void setPropertiesArrayLis(ArrayList<Property> propertiesArrayLis) {
		this.propertiesArrayLis = propertiesArrayLis;
	}

	public EntitySpec(String key, String name, Property[] properties) {
		super(key, name, properties);
		
	}

}
