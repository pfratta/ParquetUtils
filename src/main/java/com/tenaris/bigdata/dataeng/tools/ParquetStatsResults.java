package com.tenaris.bigdata.dataeng.tools;

import java.io.Serializable;

public class ParquetStatsResults implements Serializable {

	/*
	 * File statistics; number of files, total size, avg, std dev, min and max
	 * value of files in a folder
	 */

	private static final long serialVersionUID = 1L;
	
	private int numFiles;
	private double totalSize;
	private double meanSize;
	private double standardDeviation;
	private double min;
	private double max;

	public ParquetStatsResults() {
	}

	public int getNumFiles() {
		return numFiles;
	}

	public void setNumFiles(int numFiles) {
		this.numFiles = numFiles;
	}

	public double getTotalSize() {
		return totalSize;
	}

	public void setTotalSize(double totalSize) {
		this.totalSize = totalSize;
	}

	public double getMeanSize() {
		return meanSize;
	}

	public void setMeanSize(double meanSize) {
		this.meanSize = meanSize;
	}

	public double getStandardDeviation() {
		return standardDeviation;
	}

	public void setStandardDeviation(double standardDeviation) {
		this.standardDeviation = standardDeviation;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

}
