package com.tenaris.bigdata.dataeng.test;

import static org.junit.Assert.assertEquals;
import static java.lang.Math.toIntExact;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.tenaris.bigdata.dataeng.tools.BenchResultWriter;
import com.tenaris.bigdata.dataeng.tools.CSVBenchWriter;
import com.tenaris.bigdata.dataeng.tools.ParquetBenchBean;

public class CSVBenchWriterTest {

	@Test
	public void test() throws IOException, URISyntaxException {
		
		BenchResultWriter<ParquetBenchBean> writer = new CSVBenchWriter();
			
		List<ParquetBenchBean> results = new ArrayList<ParquetBenchBean>();
		results.add(new ParquetBenchBean("test", 1, 10.0));
		results.add(new ParquetBenchBean("test", 2, 20.0));
		int listsize = results.size();

		writer.writeResults(results, "csv_writer_test.csv", ";");
		
		final java.nio.file.Path path = Paths.get( "/Users/pacificofratta/Desktop/prova/csv_writer_test.csv");
		int number = toIntExact( Files.lines(path).skip(1L).count() ); // skip(1L) to ignore the titles
		
		assertEquals(listsize, number);
		
	}

}
