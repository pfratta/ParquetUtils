package com.tenaris.bigdata.dataeng.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class ParquetBench {
	
	private static String nameApp = "ParquetBench";
	private static String descriptionApp = "A command line interface tool to evaluate the performance of a query on a Parquet dataset";

	public static void main(String[] args) {

		int numberOfSamples = 10;    // default value of # samples
		String csvSeparator = " ";  // default value of .csv separator
		String outputPath = "/Users/pacificofratta/Desktop/exec_time.csv"; // default value of output path
		String expName = null;
		
		String inputPath = null;
		Configuration hdfsConfig = HDFSUtils.getConfiguration();
		Options options = configureCommandLineOptions();
		CommandLine line = CLUtils.parseCommandLine(nameApp, descriptionApp, args, options);

		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("ParquetBench");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);

		FileSystem fileSystem = null;

		try {
			fileSystem = FileSystem.get(hdfsConfig);
		} catch (IOException e) {
			System.err.println("Error while creating filesystem handler");
			e.printStackTrace();
			System.exit(1);
		}

		try {
			if (line.hasOption("i") && line.hasOption("e")) {
				inputPath = line.getOptionValue('i');
				expName = line.getOptionValue('e'); 
				if ( line.hasOption("n") && !line.hasOption("o") ) {
					numberOfSamples = Integer.parseInt(line.getOptionValue("n"));
				}
				if ( !line.hasOption("n") &&  line.hasOption("o") ) {
					outputPath = line.getOptionValue('o');
				}
				if ( line.hasOption("n") &&  line.hasOption("o") ) {
					numberOfSamples = Integer.parseInt(line.getOptionValue("n"));
					outputPath = line.getOptionValue('o');
				}
			} else {
				CLUtils.usage(nameApp, descriptionApp, options);
			}
   
			BenchResultWriter<ParquetBenchBean> writer = new CSVBenchWriter();
			ParquetBenchEngine engine = new ParquetBenchEngine(fileSystem, sqlContext);
			List<ParquetBenchBean> list = new ArrayList<ParquetBenchBean>(numberOfSamples);
			list = engine.queryTestResults(expName, inputPath, numberOfSamples);
			writer.writeResults(list, outputPath, csvSeparator);

		} catch (NumberFormatException e1) {
			System.err.println("The typed number is not valid");
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println("The path does not exist");
			System.exit(1);
		} finally {
			try {
				sc.stop();
				if (fileSystem != null) {
					fileSystem.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static Options configureCommandLineOptions() {
		Options result = new Options();

		result.addOption("i", "input", true, "specify the input directory from which to read Parquet data files");
		result.addOption("o", "output", true, "specify the output directory where to write the results file");
		result.addOption("n", "number", true, "specify the number of repetitions of a query test (10 by default)");
		result.addOption("e", "experiment-name", true, "specify the experiment name");

		return result;
	}
}