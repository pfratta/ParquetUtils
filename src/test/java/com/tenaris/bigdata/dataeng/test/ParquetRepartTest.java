package com.tenaris.bigdata.dataeng.test;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tenaris.bigdata.dataeng.tools.HDFSUtils;
import com.tenaris.bigdata.dataeng.tools.ParquetRepartEngine;

public class ParquetRepartTest {

	private static final String INPUT_DATASET_PATH = "src/test/resources/test_dataset";
	private static final String INPUT_EMPTY_DATASET_PATH = "src/test/resources/empty_test_dataset";
	private static final String OUTPUT_DIR_PATH = "temp/test/repartitioned";

	private static JavaSparkContext sc;
	private static SQLContext sqlContext;
	private static FileSystem fileSystem;
	private static ParquetRepartEngine engine;

	@BeforeClass
	public static void createContext() throws IOException {

		Configuration hdfsConfig = HDFSUtils.getConfiguration();
		SparkConf config = new SparkConf();
		config.setMaster("local[*]");
		config.setAppName("my JUnit running Spark");
		sc = new JavaSparkContext(config);
		fileSystem = FileSystem.get(hdfsConfig);
		sqlContext = new SQLContext(sc);
		engine = new ParquetRepartEngine(fileSystem, sqlContext);
	}

	@Test
	public void repartitionByNumberEverythingOk() throws IOException {

		int numberOfParquet = 3;
		boolean overwrite = false;

		Path p = new Path(OUTPUT_DIR_PATH);
		fileSystem.delete(p, true);

		engine.repartitionByNumberOfFile(INPUT_DATASET_PATH, OUTPUT_DIR_PATH, numberOfParquet, overwrite);

		FileStatus[] files = fileSystem.listStatus(p);

		int endingInParquet = 0;

		for (FileStatus fileStatus : files) {
			if (fileStatus.getPath().getName().endsWith(".parquet"))
				endingInParquet++;
		}

		assertEquals(numberOfParquet, endingInParquet);
	}

	@Test(expected = IOException.class)
	public void repartitionByNumberOutputNotEmpty() throws IOException {

		int numberOfParquet = 3;
		boolean overwrite = false;

		Path p = new Path(OUTPUT_DIR_PATH);
		fileSystem.delete(p, false);
		fileSystem.mkdirs(p);
		fileSystem.copyFromLocalFile(new Path(INPUT_DATASET_PATH), p);

		engine.repartitionByNumberOfFile(INPUT_DATASET_PATH, OUTPUT_DIR_PATH, numberOfParquet, overwrite);

	}

	@Test(expected = FileNotFoundException.class)
	public void repartitionByNumberEmptyInput() throws IOException {

		int numberOfParquet = 3;
		boolean overwrite = true;

		engine.repartitionByNumberOfFile(INPUT_EMPTY_DATASET_PATH, OUTPUT_DIR_PATH, numberOfParquet, overwrite);

	}

	@AfterClass
	public static void closeContext() throws IOException {
		if (sc != null) {
			sc.stop();
		}

		if (fileSystem != null) {
			fileSystem.close();
		}
	}

}
