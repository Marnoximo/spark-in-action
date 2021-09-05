package net.tqtien.ch03;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.split;

public class IngestSchemaChangeJsonApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IngestSchemaChangeJsonApp app = new IngestSchemaChangeJsonApp();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder()
				.appName("Ingest JSON change schema")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> df = spark.read()
				.format("json")
				.load("data/Restaurants_in_Durham_County_NC.json");
		
		df.printSchema();
		df = df.drop("datasetid");
		
		df = df.withColumn("county", lit("Durham"))
				.withColumn("datasetId", df.col("fields.id"))
				.withColumn("address1", df.col("fields.premise_address1"))
				.withColumn("address2", df.col("fields.premise_address2"))
				.withColumn("phone", df.col("fields.premise_phone"))
				.withColumn("state", df.col("fields.premise_state"))
				.withColumn("zip", df.col("fields.premise_zip"))
				.withColumn("datestart", df.col("fields.opening_date"))
				.withColumn("dateEnd", df.col("fields.closing_date"))
				.withColumn("type", split(df.col("fields.type_description"), " - ").getItem(1))
				.withColumn("geoX", df.col("fields.geolocation").getItem(0))
				.withColumn("geoY", df.col("fields.geolocation").getItem(1));
		
		df = df.withColumn("id", concat(
				df.col("state"), lit("_"),
				df.col("county"), lit("_"),
				df.col("datasetId")
				));
		
		String[] drop_cols = {"fields", "geometry", "record_timestamp", "recordid"};
		df = df.drop(drop_cols);
		
		df.printSchema();
		df.show(5);
	}
}
