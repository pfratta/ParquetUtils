package com.tenaris.bigdata.dataeng.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

public class ParquetGenerator {

	private static String nameApp = "ParquetGenerator";
	private static String descriptionApp = "A command line interface tool that generate a random dataset";

	public static void main(String[] args) throws IOException {

		Configuration hdfsConfig = HDFSUtils.getConfiguration();
		Options options = configureCommandLineOptions();
		CommandLine line = CLUtils.parseCommandLine(nameApp, descriptionApp, args, options);
		List<String> names = null;

		// Creo un context Spark
		SparkConf sparkConf = new SparkConf().setAppName("ParquetGenerator");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);

		Random seedGenerator = new Random();
		int numRows = 0, // number of rows of single file
				numPart = 0; // number of Parquet files

		FileSystem fileSystem = null;

		try {
			fileSystem = FileSystem.get(hdfsConfig);
		} catch (IOException e) {
			System.err.println("Error while creating filesystem handler");
			e.printStackTrace();
			System.exit(1);
		}

		try {
			if (line.hasOption("s") && line.hasOption("p") && line.hasOption("r") && line.hasOption("o")) {

				// Verifico che il file di configurazione esista
				java.nio.file.Path path = Paths.get(line.getOptionValue('s'));
				if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
					System.err.println("The path of configuration file is wrong");
					System.exit(1);
				}

				// Lo leggo come file .properties
				Properties properties = new Properties();
				properties.load(new FileReader(line.getOptionValue('s')));

				// Leggo il path di output
				String output = line.getOptionValue('o');

				// Leggo il numero di partizioni
				numPart = Integer.parseInt(line.getOptionValue('p'));
				if (numPart <= 0) {
					System.err.println("The number of Parquet files must be positive");
					System.exit(1);
				}

				// Leggo il numero di righe
				numRows = Integer.parseInt(line.getOptionValue('r'));
				if (numRows <= 0) {
					System.err.println("The number of rows in each file must be positive");
					System.exit(1);
				}
				
				Map<String, String> mappa = ParquetGenerator.propertiesToMap(properties);

				// Se la mappa contiene il tipo dictionary e l'utente ha
				// specificato un file dictionary, controlla che il file esiste
				// e poi leggi la lista di nomi che contiene
				if (line.hasOption("f")) {
					java.nio.file.Path pathOfRandomNames = Paths.get(line.getOptionValue('f'));
					if (!Files.exists(pathOfRandomNames, LinkOption.NOFOLLOW_LINKS)) {
						System.err.println("The path of dictionary is wrong");
						System.exit(1);
					}
					names = ParquetGenerator.namesGeneratorFromFile(line.getOptionValue('f'));
				}
				// Se la mappa contine il tipo dictionary ma l'utente non ha
				// specificato un file dictionary, creane uno
				else {
					names = Arrays.asList("Mario", "Vincenzo", "Andrea", "Carlo", "Fabio");
				}

				// Dichiaro una lista di oggetti bean che descrivono la
				// struttura del
				// dataset
				List<ParquetGeneratorBean> structOfDataset = new ArrayList<ParquetGeneratorBean>(numPart);

				// Creo e inserisco nella lista un numero di oggetti pari al
				// numero di
				// partizioni che intendo memorizzare
				for (int i = 0; i < numPart; i++) {
					structOfDataset.add(new ParquetGeneratorBean(numRows, seedGenerator.nextLong(), mappa));
				}
				// Creo un RDD dalla lista e gli applico la flatMap
				JavaRDD<ParquetGeneratorBean> rdd = sc.parallelize(structOfDataset, numPart);
				// System.out.println(rdd.count());
				JavaRDD<Row> rowRDD = rdd.flatMap(new ParquetGeneratorEngine(names));

				// System.out.println(rowRDD.count());

				// Definisco lo schema del dataframe
				StructType struct = ParquetGeneratorEngine.datasetSchema(mappa);

				DataFrame dataInput = sqlContext.createDataFrame(rowRDD, struct);
				dataInput.write().mode("overwrite").parquet(output.toString());

				//dataInput.show();

			} else {
				CLUtils.usage(nameApp, descriptionApp, options);
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
		result.addOption("s", "structPath", true,
				"specify the input file path containing the structure of dataset in the form of key=value pairs");
		result.addOption("f", "structPath", true, "specify the path of a file .txt containing a set of values");
		result.addOption("p", "partitions", true, "number of output Parquet data files");
		result.addOption("r", "rows", true, "number of rows of each Parquet data file");
		result.addOption("o", "output", true, "specify the output directory in which to store Parquet data files");
		return result;
	}

	public static List<String> namesGeneratorFromFile(String path) throws IOException {
		BufferedReader br = null;
		List<String> list = null;
		try {
			br = new BufferedReader(new FileReader(path));
			list = new ArrayList<String>();
			String s;
			while ((s = br.readLine()) != null) {
				list.add(s);
			}
		} finally {
			br.close();
		}

		return list;
	}
	
	public static Map<String, String> propertiesToMap(Properties props) {

		LinkedHashMap<String, String> convToMap = new LinkedHashMap<String, String>();
		Set<String> propertyNames = props.stringPropertyNames();

		for (String Property : propertyNames) {
			convToMap.put(Property, props.getProperty(Property));
		}
		return convToMap;
	}

}
