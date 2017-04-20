package com.tenaris.bigdata.dataeng.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ParquetGeneratorEngine implements FlatMapFunction<ParquetGeneratorBean, Row> {
	private static final long serialVersionUID = 1L;
	
	protected List<String> names;
	
	public ParquetGeneratorEngine() { }

	public ParquetGeneratorEngine( List<String> names) {
		this.names = names;
	}

	public Iterable<Row> call(ParquetGeneratorBean x) throws IOException {
		List<Row> result = new ArrayList<Row>(x.getNumRecord());
		Random random = new Random(x.getSeed());
		for (int i = 0; i < x.getNumRecord(); i++) {
			List<Object> temp = new ArrayList<Object>(x.getStructRecord().size());
			for (Map.Entry<String, String> entry : x.getStructRecord().entrySet()) {

				switch (entry.getValue().toLowerCase()) {
				case "string":
					temp.add(RandomStringUtils.randomAlphabetic(5));
					break;
				case "int":
					temp.add(random.nextInt());
					break;
				case "double":
					temp.add(random.nextDouble());
					break;
				case "dictionary":
					temp.add(names.get(random.nextInt(names.size())));
					break;

				}
			}
			if (!temp.isEmpty()) {
				result.add(RowFactory.create(temp.toArray()));
			}
		}
		return result;

	}

	static public StructType datasetSchema(Map<String, String> mappa) {
		StructType struct = new StructType();
		for (Map.Entry<String, String> entry : mappa.entrySet()) {
			switch (entry.getValue().toLowerCase()) {
			case "string":
			case "dictionary":
				struct = struct.add(entry.getKey(), DataTypes.StringType);
				break;
			case "int":
				struct = struct.add(entry.getKey(), DataTypes.IntegerType);
				break;
			case "double":
				struct = struct.add(entry.getKey(), DataTypes.DoubleType);
				break;
			}
		}
		return struct;
	}
}
