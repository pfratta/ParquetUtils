package com.tenaris.bigdata.dataeng.tools;

import java.io.IOException;
import java.util.List;

public interface BenchResultWriter<T> {

	void writeResults(List<T> list, String outputPath, String separator) throws IOException;

}