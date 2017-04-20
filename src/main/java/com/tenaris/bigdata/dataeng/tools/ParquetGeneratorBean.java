package com.tenaris.bigdata.dataeng.tools;

import java.io.Serializable;
import java.util.Map;

public class ParquetGeneratorBean implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private int numRecord;
	Map <String, String> structRecord;
	private long seed;

	public ParquetGeneratorBean(int numRecord, long seed, Map <String, String> mappa) {
		this.numRecord = numRecord;
		this.structRecord = mappa;
		// Generate a random number 0 ≤ seed < ext_upper
		this.seed = seed;
	}

	public int getNumRecord() {
		return numRecord;
	}

	public void setNumRecord(int numRecord) {
		this.numRecord = numRecord;
	}

	public Map<String, String> getStructRecord() {
		return structRecord;
	}

	public void setStructRecord(Map<String, String> structRecord) {
		this.structRecord = structRecord;
	}

	public long getSeed() {
		return seed;
	}

	public void setSeed(long seed) {
		this.seed = seed;
	}

}