package com.tenaris.bigdata.dataeng.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class ParquetBenchEngine {

	protected SQLContext sqlContext;
	protected FileSystem fileSystem;

	public ParquetBenchEngine(FileSystem fileSystem, SQLContext sqlContext) {
		this.fileSystem = fileSystem;
		this.sqlContext = sqlContext;
	}

	public List<ParquetBenchBean> queryTestResults(String nameOfStrategy, String inputPath, int numOfSample,
			QueryStrategy qc) throws IOException {

		// Read the input path
		Path input = new Path(inputPath);

		// Check if the input folder is empty
		if (HDFSUtils.containsFiles(fileSystem, new Path(inputPath), ".parquet")) {
			throw new FileNotFoundException("The folder " + inputPath + " does not contain any .parquet file");
		}

		long start, end;
		int sampleId = 1;
		double executionTime;
		DataFrame dataInput = sqlContext.read().parquet(input.toString());
		List<ParquetBenchBean> list = new ArrayList<ParquetBenchBean>();
		
	    Class<? extends QueryStrategy> qcClass = qc.getClass();

	    // returns the name of the class
	    nameOfStrategy = qcClass.getSimpleName();
	    System.out.println("Name of strategy: " + nameOfStrategy);
		for (int i = 0; i < numOfSample; i++) {
			System.out.println("Iteration # " + (i + 1) + " of " + numOfSample);
			start = System.nanoTime();

			qc.doQuery(dataInput);

			end = System.nanoTime();
			executionTime = ((double) (end - start) / 1E6);
			list.add(new ParquetBenchBean(nameOfStrategy, sampleId, executionTime));
			sampleId++;
		}

		return list;

	}

}