package com.tenaris.bigdata.dataeng.tools;

import java.io.Serializable;

public class ParquetBenchBean implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String nameOfStrategy;
	private int sampleID;
	private double executionTime;
	
	public ParquetBenchBean() {}
	
	public ParquetBenchBean(String nameOfStrategy, int sampleId, double executionTime) {
		this.nameOfStrategy = nameOfStrategy;
		this.sampleID = sampleId;
		this.executionTime = executionTime;
	}

	public String getNameOfStrategy() {
		return nameOfStrategy;
	}

	public void setNameOfStrategy(String nameOfStrategy) {
		this.nameOfStrategy = nameOfStrategy;
	}

	public int getSampleId() {
		return sampleID;
	}

	public void setSampleId(int sampleId) {
		this.sampleID = sampleId;
	}

	public double getExecutionTime() {
		return executionTime;
	}

	public void setExecutionTime(double executionTime) {
		this.executionTime = executionTime;
	}

}
