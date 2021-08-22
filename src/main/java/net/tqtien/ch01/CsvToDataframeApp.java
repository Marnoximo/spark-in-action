package net.tqtien.ch01;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class CsvToDataframeApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CsvToDataframeApp app = new CsvToDataframeApp();
		app.start();
	}

	private void start() {
		SparkSession spark = new SparkSession.Builder()
			.appName("Read CSV")
			.master("local")
			.getOrCreate();
		
		Dataset<Row> dataframe = spark.read().format("csv")
			.option("header", "true")
			.load("data/books.csv");
		
		dataframe.take(10);
		dataframe.printSchema();
		dataframe.show(5);
	}
}
