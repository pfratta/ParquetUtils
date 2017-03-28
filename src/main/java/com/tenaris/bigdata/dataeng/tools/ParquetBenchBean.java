package com.tenaris.bigdata.dataeng.tools;

import java.io.Serializable;

public class ParquetBenchBean implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private int numberOfSamples;
	private double executionTimes;
	private String expName;
	
	public ParquetBenchBean() {}
	
	public ParquetBenchBean(String expName, int numberOfSamples, double executionTimes) {
		this.expName = expName;
		this.numberOfSamples = numberOfSamples;
		this.executionTimes = executionTimes;
	}

	public int getNumberOfSamples() {
		return numberOfSamples;
	}

	public void setNumberOfSamples(int numberOfSamples) {
		this.numberOfSamples = numberOfSamples;
	}

	public double getExecutionTimes() {
		return executionTimes;
	}

	public void setExecutionTimes(double executionTimes) {
		this.executionTimes = executionTimes;
	}

	public String getExpName() {
		return expName;
	}

	public void setExpName(String expName) {
		this.expName = expName;
	}

}
