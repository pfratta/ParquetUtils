package com.tenaris.bigdata.dataeng.tools;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class ParquetRepart {

	private static String nameApp = "ParquetRepart";
	private static String descriptionApp = "A command line interface tool to redistribute a set of Parquet data files";

	public static void main(String[] args) {

		Configuration hdfsConfig = HDFSUtils.getConfiguration();
		Options options = configureCommandLineOptions();
		CommandLine line = HDFSUtils.parseCommandLine(nameApp, descriptionApp, args, options);

		SparkConf sparkConf = new SparkConf(); //.setMaster("local[*]").setAppName("ParquetRepart");
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
			
		ParquetRepartEngine engine = new ParquetRepartEngine(fileSystem, sqlContext);

		int sizeParquet = 0, numberOfParquet = 0;

		try {
			if (line.hasOption("i") && line.hasOption("o")) {

				// Leggo il path di input
				String input = line.getOptionValue('i');

				// Leggo il path di output
				String output = line.getOptionValue('o');
				
				if ( input.equals(output) ) {
					System.out.println(" Input and output path must be different");
					System.exit(1);
				}

				if (line.hasOption("p") && !line.hasOption("s")) {
					numberOfParquet = Integer.parseInt(line.getOptionValue('p'));
					if (numberOfParquet <= 0) {
						System.err.println("The number of Parquet files must be positive");
						System.exit(1);
					}

					engine.repartitionByNumberOfFile(input, output, numberOfParquet, line.hasOption("f"));
				

				} else if (line.hasOption("s") && !line.hasOption("p")) {
					sizeParquet = Integer.parseInt(line.getOptionValue('s'));
					if (sizeParquet <= 0) {
						System.err.println("The mean size of Parquet file must be positive");
						System.exit(1);
					}
					
					engine.repartitionByMinDimension( input, output, sizeParquet, line.hasOption("f") );
				
				} else {
					HDFSUtils.usage(nameApp, descriptionApp, options);
				}

			} else {
				HDFSUtils.usage(nameApp, descriptionApp, options);
			}
		} catch (NumberFormatException e1) {
			System.err.println("The typed number is not valid");
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				sc.stop();
				fileSystem.close();			
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private static Options configureCommandLineOptions() {
		Options result = new Options();
		result.addOption("i", "input", true, "specify the input directory from which to read Parquet data files");
		result.addOption("o", "output", true,
				"specify the output directory in which to store restructured Parquet data files");
		result.addOption("p", "partitions", true, "number of output data files");
		result.addOption("f", "flagoverwrite", false, "flag indicating overwrite mode");
		result.addOption("s", "size", true, "size of output Parquet data files");
		return result;
	}
}
