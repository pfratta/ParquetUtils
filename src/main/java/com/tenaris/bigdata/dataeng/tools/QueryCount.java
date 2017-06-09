package com.tenaris.bigdata.dataeng.tools;

import org.apache.spark.sql.DataFrame;

public class QueryCount implements QueryStrategy {
	
	private String colTarget;
	
	public QueryCount (String colTarget) {
		System.out.println("ho chiamato il cost. querycount");
		this.colTarget = colTarget;
	}
	
	@Override
	public void doQuery (DataFrame dataInput) {
		dataInput.select(colTarget)
		.distinct()
		.count();
	}

}
