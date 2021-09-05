package net.tqtien.ch03;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;


public class IngestSchemaChangeCsvApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IngestSchemaChangeCsvApp app = new IngestSchemaChangeCsvApp();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder()
				.appName("Ingest CSV change schema")
				.master("local")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", "true")
				.load("data/Restaurants_in_Wake_County_NC.csv");

		System.out.println("Original schema");
		df.printSchema();

		df = this.drop_columns(df);
		System.out.println("Schema after dropping columns");
		df.printSchema();
		
		df = this.rename_columns(df);
		System.out.println("Schema after renaming columns");
		df.printSchema();
		
		df = this.add_columns(df);
		System.out.println("Schema after adding columns");
		df.printSchema();
		
		df.show(5);
		
		System.out.printf("Partitions: %d\n", df.rdd().partitions().length);
		df = df.repartition(4);
		System.out.printf("Partitions: %d\n", df.rdd().partitions().length);
	}
	
	private Dataset<Row> drop_columns(Dataset<Row> df) {
		String[] DROP_COLS = { "OBJECTID", "PERMITID", "GEOCODESTATUS" };
		return df.drop(DROP_COLS);
	}
	
	private Dataset<Row> rename_columns(Dataset<Row> df) {
		return df.withColumnRenamed("HSISID", "datasetID")
				.withColumnRenamed("NAME", "name")
				.withColumnRenamed("ADDRESS1", "address1")
				.withColumnRenamed("ADDRESS2", "address2")
				.withColumnRenamed("CITY", "city")
				.withColumnRenamed("STATE", "state")
				.withColumnRenamed("POSTALCODE", "zip")
				.withColumnRenamed("PHONENUMBER", "tel")
				.withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
				.withColumnRenamed("FACILITYTYPE", "type")
				.withColumnRenamed("X", "geoX")
				.withColumnRenamed("Y", "geoY");
	}
	
	private Dataset<Row> add_columns(Dataset<Row> df) {
		df = df.withColumn("county", lit("Wake"))
				.withColumn("dateEnd", lit(null).cast("string"));
		return df.withColumn("id", concat(
				df.col("state"), lit("_"),
				df.col("county"), lit("_"),
				df.col("datasetId")
				));
	}
}
