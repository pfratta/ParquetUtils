package com.tenaris.bigdata.dataeng.tools;

import org.apache.spark.sql.DataFrame;

public interface QueryStrategy {
	public void doQuery (DataFrame dataInput);
}
