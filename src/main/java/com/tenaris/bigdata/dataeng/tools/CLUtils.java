package com.tenaris.bigdata.dataeng.tools;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CLUtils {
	
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
