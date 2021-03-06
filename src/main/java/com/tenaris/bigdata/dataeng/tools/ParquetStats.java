package com.tenaris.bigdata.dataeng.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

public class ParquetStats {

	private static String nameApp = "ParquetStats";
	private static String descriptionApp = "A command line interface tool to compute statistics from a set of files";

	public static void main(String[] args) throws IOException {

		FileSystem fileSystem = null;
		Path inputPath = null;
		Configuration conf = null;
		configureLog4jFromSystemProperties();
		ParquetStatsBean pst = new ParquetStatsBean();

		Options options = configureCommandLineOptions();
		CommandLine line = CLUtils.parseCommandLine(nameApp, descriptionApp, args, options);

		final int BYTES_IN_MB = (int) Math.pow(2, 20);

		try {
			if ( line.hasOption("i") ) {
				inputPath = new Path(line.getOptionValue('i'));
				if (line.hasOption("c") ) {
					conf = HDFSUtils.getConfiguration(line.getOptionValue("c"));
				} else {
					conf = HDFSUtils.getConfiguration();	
				}
			}
			else {
				CLUtils.usage(nameApp, descriptionApp, options);
			}
			
			fileSystem = FileSystem.get(conf);
			
			if (HDFSUtils.containsFiles(fileSystem, inputPath, ".parquet")) {
				System.err.println("The folder " + inputPath + " does not contain any .parquet file");
				System.exit(1);
			}

			pst = statistics(fileSystem, inputPath, ".parquet");

			// Number of files in the folder
			System.out.printf("Number of .parquet files: %d\n", pst.getNumFiles());

			// Total size
			System.out.printf("Total size: %.5f MB\n", pst.getTotalSize() / BYTES_IN_MB);

			// Average
			System.out.printf("Mean size: %.5f MB\n", pst.getMeanSize() / BYTES_IN_MB);

			// Standard Deviation
			System.out.printf("Standard deviation: %.5f MB\n", pst.getStandardDeviation() / BYTES_IN_MB);

			// Min value
			System.out.printf("Min: %.5f MB\n", pst.getMin() / BYTES_IN_MB);

			// Max value
			System.out.printf("Max: %.5f MB\n", pst.getMax() / BYTES_IN_MB);

		} catch (Exception e) {
			System.err.println("Tha path does not exist");
			System.exit(1);
		} finally {
			try {
				// sc.stop();
				if(fileSystem != null) {
					fileSystem.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static ParquetStatsBean statistics(FileSystem fs, Path input, String fileExtension)
			throws FileNotFoundException, IOException {

		FileStatus[] status = fs.listStatus(input);
		ParquetStatsBean par = new ParquetStatsBean();
		String pathHDFS;
		int numFiles = 0;
		double totalSize = 0.0, meanSize = 0.0, standardDeviation = 0.0, minValue = Double.MAX_VALUE, maxValue = 0.0,
				currentValue = 0.0, sumOfSquaredDiff = 0.0;

		for (FileStatus f : status) {
			pathHDFS = f.getPath().toString();
			if (pathHDFS.endsWith(fileExtension)) {
				numFiles += 1;
				totalSize += f.getLen(); // Get the length of this file in bytes
				currentValue = f.getLen();
				minValue = Math.min(minValue, currentValue);
				maxValue = Math.max(maxValue, currentValue);

			}
		}

		meanSize = (totalSize / numFiles);

		for (FileStatus f : status) {
			pathHDFS = f.getPath().toString();
			if (pathHDFS.endsWith(fileExtension)) {
				sumOfSquaredDiff += Math.pow(f.getLen() - meanSize, 2);
			}
		}

		standardDeviation = (Math.sqrt(sumOfSquaredDiff / (numFiles - 1)));

		par.setNumFiles(numFiles);
		par.setTotalSize(totalSize);
		par.setMeanSize(meanSize);
		par.setStandardDeviation(standardDeviation);
		par.setMin(minValue);
		par.setMax(maxValue);

		return par;

	}

	private static Options configureCommandLineOptions() {
		Options result = new Options();
		result.addOption("i", "input", true, "specify the directory containing Parquet data files");
		result.addOption("c", "config", true, "specify the xml file path containing namenode address and port");

		return result;
	}

	public static void configureLog4jFromSystemProperties() {
		final String LOGGER_PREFIX = "log4j";
		Properties props = new Properties();

		System.out.println("<Log_level>: DEBUG, WARN, ERROR, FATAL, INFO, TRACE, TRACE_INT, ALL, OFF");
		System.out.print(
				"Type \"-Dlog4j=<Log_level>\" as VM arguments to specify a log level - By default \"debug\" mode is on\n\n");

		// Read the VM argument (e.g. java -Dlog4j=all ...)
		for (String propertyName : System.getProperties().stringPropertyNames()) {

			if (propertyName.startsWith(LOGGER_PREFIX)) {

				/*
				 * take the value on the right of equal sign of VM argument and
				 * store it in "levelName" variable (e.g. java -Dlog4j=all, take
				 * the "all" value and store it in "levelName" variable)
				 */
				String levelName = System.getProperty(propertyName, "");

				Level level = Level.toLevel(levelName); // If the conversion
														// fails, then this
														// method returns DEBUG

				// If a value is assigned to log4j option and this value is not
				// recognized by Level class
				if (!"".equals(levelName) && !levelName.toUpperCase().equals(level.toString())) {
					System.err.print("Skipping unrecognized log4j log level " + levelName + ": -D" + propertyName + "="
							+ levelName + "\n\n");
				} else {
					try {
						InputStream configStream = ParquetStats.class.getClassLoader()
								.getResourceAsStream("log4j.properties");
						props.load(configStream);
						configStream.close();
					} catch (Exception e) {
						System.err.println("Error not laod configuration file");
						System.exit(1);
					}
					props.setProperty("log4j.rootLogger", levelName + ", stdout");
					LogManager.resetConfiguration();
					PropertyConfigurator.configure(props);
				}
			}
		}
	}
}
