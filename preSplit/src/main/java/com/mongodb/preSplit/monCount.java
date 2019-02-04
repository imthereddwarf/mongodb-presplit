package com.mongodb.preSplit;

public class monCount {
	
	private final String month;
	private final Long count;
	
	public monCount(String monthIn,Long countIn) {
		month = monthIn;
		count = countIn;
	}

	public String getMonth() {
		return month;
	}

	public Long getCount() {
		return count;
	}
	

}