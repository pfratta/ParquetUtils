package com.tenaris.bigdata.dataeng.tools;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions;

public class QueryPow implements QueryStrategy {
	
	private String colInput;
	private String colOutput;
	
	public QueryPow (String colInput, String colOutput) {
		System.out.println("ho chiamato il cost. querypow");
		this.colInput = colInput;
		this.colOutput = colOutput;
	}
	
	@Override
	public void doQuery (DataFrame dataInput) {
		dataInput.withColumn(colOutput, functions.pow(dataInput.col(colInput), 2))
		.select(colInput, colOutput)
		.agg(functions.sum(colOutput))
		.first();
	}

}
