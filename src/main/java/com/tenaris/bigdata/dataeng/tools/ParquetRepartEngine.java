package com.tenaris.bigdata.dataeng.tools;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class ParquetRepartEngine {

	protected static final int BYTES_IN_MB = (int) Math.pow(2, 20);
	protected SQLContext sqlContext;
	protected ParquetStats ps;
	protected FileSystem fileSystem;

	public ParquetRepartEngine(FileSystem fileSystem, SQLContext sqlContext) {
		this.fileSystem = fileSystem;
		this.sqlContext = sqlContext;
	}

	public void repartitionByMinDimension(String inputPath, String outputPath, long sizeParquet, boolean overwrite)
			throws FileNotFoundException, IOException {

		Double totalSize = ParquetStats.statistics(fileSystem, new Path(inputPath), ".parquet").getTotalSize();

		int numberOfParquet = (int) (totalSize / sizeParquet);

		repartitionByNumberOfFile(inputPath, outputPath, numberOfParquet, overwrite);
	}

	public void repartitionByNumberOfFile(String inputPath, String outputPath, int numberOfParquet, boolean overwrite)
			throws IOException {

		// Leggo il path di input
		Path input = new Path(inputPath);

		// Leggo il path di output
		Path output = new Path(outputPath);

		// Controllo se la cartella di input è vuota
		if (HDFSUtils.containsFiles(fileSystem, input, ".parquet")) {
			throw new FileNotFoundException("The folder " + inputPath + " does not contain any .parquet file");
		}

		// Se la cartella di output esiste, non è vuota e non si è specificato
		// la sovrascrittura
		if (overwrite == false && fileSystem.exists(output)
				&& !HDFSUtils.containsFiles(fileSystem, output, ".parquet")) {
			throw new IOException("The output folder " + outputPath + " already contains parquet files");
		}

		DataFrame dataInput = sqlContext.read().parquet(input.toString());

		dataInput.repartition(numberOfParquet).write().mode(overwrite ? SaveMode.Overwrite : SaveMode.ErrorIfExists)
				.parquet(output.toString());

	}

}
