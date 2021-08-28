package net.tqtien.ch02;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


public class WriteToPostgresApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		WriteToPostgresApp app = new WriteToPostgresApp();
		app.start();
	}
	
	private void start() {
		SparkSession spark = SparkSession.builder()
				.appName("Write to PostgreSQL")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.load("data/authors.csv");
		
		df.printSchema();
		
		df = df.withColumn(
				"name",
				concat(df.col("lname"), lit(' '), df.col("fname")));
		
		df.show(5);
		
		String connectionUrl = "jdbc:postgresql://127.0.0.1/spark_in_action";
		
		Properties prop = new Properties();
		prop.setProperty("driver", "org.postgresql.Driver");
		prop.setProperty("user", "spark");
		prop.setProperty("password", "spark");
		
		df.write()
			.mode(SaveMode.Overwrite)
			.jdbc(connectionUrl, "ch02_table" ,prop);
		
//		df.write()
//        .mode(SaveMode.Overwrite)
//        .option("dbtable", "ch02_900_table")
//        .option("url", "jdbc:postgresql://localhost/spark_in_action")
//        .option("driver", "org.postgresql.Driver")
//        .option("user", "spark")
//        .option("password", "spark")
//        .format("jdbc")
//        .save();
	}
}
