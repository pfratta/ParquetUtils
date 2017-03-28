package com.tenaris.bigdata.dataeng.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;

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
	
	public static Configuration getConfiguration(String pathConf) {

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

		java.nio.file.Path path = Paths.get(pathConf);
		if( Files.exists(path, LinkOption.NOFOLLOW_LINKS) ) {
			conf.addResource(pathConf);
		} else {
			System.err.println("The path of namenode configuration file is wrong");
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
}
