package com.tenaris.bigdata.dataeng.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtils {

	public static Configuration getConfiguration() {

		// System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();

		/*
		 * The following two instructions resolve the file system overwriting
		 * issues (cfr.
		 * http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-
		 * scheme-file)
		 */
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		InputStream inputStream = ParquetStats.class.getClassLoader().getResourceAsStream("core-site.xml");
		if (inputStream != null) {
			conf.addResource(inputStream);
		} else {
			System.err.println("Cannot find core-site.xml in java classpath");
			System.exit(1);
		}
		return conf;
	}

	public static boolean containsFiles(FileSystem fs, Path dir, String fileExtension)
			throws FileNotFoundException, IOException {

		FileStatus[] status = fs.listStatus(dir);

		String pathHDFS;
		for (FileStatus f : status) {
			pathHDFS = f.getPath().toString();
			if (pathHDFS.endsWith(fileExtension)) {
				return false;
			}
		}

		return true;
	}

	public static CommandLine parseCommandLine(String nameApp, String descriptionApp, String[] args, Options options) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			usage(nameApp, descriptionApp, options);
		}
		return cmd;
	}

	public static void usage(String nameApp, String descriptionApp, Options options) {

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(nameApp, descriptionApp, options, "");
		System.exit(1);
	}
}
