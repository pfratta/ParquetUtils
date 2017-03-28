package com.tenaris.bigdata.dataeng.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

public class ParquetBenchEngine {

	protected SQLContext sqlContext;
	protected FileSystem fileSystem;

	public ParquetBenchEngine(FileSystem fileSystem, SQLContext sqlContext) {
		this.fileSystem = fileSystem;
		this.sqlContext = sqlContext;
	}

	public List<ParquetBenchBean> queryTestResults(String expName, String inputPath, int numberOfSamples) throws IOException {

		// Leggo il path di input
		Path input = new Path(inputPath);

		// Controllo se la cartella di input è vuota
		if (HDFSUtils.containsFiles(fileSystem, new Path(inputPath), ".parquet")) {
			throw new FileNotFoundException("The folder " + inputPath + " does not contain any .parquet file");
		}

		long start, end;
		int round = 1;
		double executionTime;
		DataFrame dataInput = sqlContext.read().parquet(input.toString());
		List<ParquetBenchBean> list = new ArrayList<ParquetBenchBean>();
		
		for (int i = 0; i < numberOfSamples; i++) {
			System.out.println("Iteration # " + (i + 1) + " of " + numberOfSamples);
			start = System.nanoTime();
			
			dataInput.withColumn("Foo", functions.pow(dataInput.col("ManufacturingWt"), 2))
					.select("ManufacturingWt", "Foo")
					.agg(functions.sum("Foo"))
					.first();
			
			end = System.nanoTime();
			executionTime = ( (double) (end - start) / 1E6 );
			list.add( new ParquetBenchBean(expName, round, executionTime) );
			round++;
		}

		return list;

	}

}
