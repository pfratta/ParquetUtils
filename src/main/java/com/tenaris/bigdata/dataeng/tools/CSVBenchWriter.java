package com.tenaris.bigdata.dataeng.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CSVBenchWriter implements BenchResultWriter<ParquetBenchBean> {

	private static final String FILE_HEADER = "exp_name round exec_time(ms)";
	private static final String NEW_LINE_SEPARATOR = "\n";

	public void writeResults(List<ParquetBenchBean> list, String outputPath, String separator) throws IOException {

		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(outputPath);
			fileWriter.append(FILE_HEADER.toString());
			fileWriter.append(NEW_LINE_SEPARATOR);

			for (ParquetBenchBean d : list) {
				fileWriter.append(d.getExpName());
				fileWriter.append(separator);
				fileWriter.append(String.valueOf(d.getNumberOfSamples()));
				fileWriter.append(separator);
				fileWriter.append(String.valueOf(d.getExecutionTimes()));
				fileWriter.append(NEW_LINE_SEPARATOR);
			}
		} catch (IOException e) {
			System.err.println("Error while created File " + outputPath);
			throw e;
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				System.err.println("Error while flushing/closing fileWriter");
				e.printStackTrace();
			}
		}
	}
}
