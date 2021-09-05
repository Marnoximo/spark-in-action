package net.tqtien.ch03;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.split;

public class JoinDataframeApp {
	private SparkSession spark;
	
	public static void main(String[] args) {
		JoinDataframeApp app = new JoinDataframeApp();
		app.start();
		Dataset<Row> dfWake = app.buildDataframeWake();
		System.out.printf("Dataframe Wake: %d rows\n", dfWake.count());
		
		Dataset<Row> dfDurham = app.buildDataframeDurham();
		System.out.printf("Dataframe Durham: %d rows\n", dfDurham.count());
		
		Dataset<Row> dfUnion = app.unionDataframes(dfWake, dfDurham);
		System.out.printf("Dataframe unioned: %d rows\n", dfUnion.count());
		dfUnion.printSchema();
		dfUnion.show(5);
	}
	
	private void start( ) {
		this.spark = SparkSession.builder()
				.appName("Join dataframe")
				.master("local")
				.getOrCreate();
	}
	
	private Dataset<Row> buildDataframeWake() {
		Dataset<Row> df = this.spark.read()
				.format("csv")
				.option("header", "true")
				.load("data/Restaurants_in_Wake_County_NC.csv");
		
		String[] DROP_COLS = { "OBJECTID", "PERMITID", "GEOCODESTATUS" };
		df = df.drop(DROP_COLS);
		df = df.withColumnRenamed("HSISID", "datasetID")
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
		df = df.withColumn("county", lit("Wake"))
				.withColumn("dateEnd", lit(null).cast("string"));
		df = df.withColumn("id", concat(
				df.col("state"), lit("_"),
				df.col("county"), lit("_"),
				df.col("datasetId")
				));
		
		return df;
	}
	
	private Dataset<Row> buildDataframeDurham() {
		Dataset<Row> df = spark.read()
				.format("json")
				.load("data/Restaurants_in_Durham_County_NC.json");

		df = df.drop("datasetid");		
		df = df.withColumn("county", lit("Durham"))
				.withColumn("name", df.col("fields.premise_name"))
				.withColumn("datasetId", df.col("fields.id"))
				.withColumn("address1", df.col("fields.premise_address1"))
				.withColumn("address2", df.col("fields.premise_address2"))
				.withColumn("phone", df.col("fields.premise_phone"))
				.withColumn("city", df.col("fields.premise_city"))
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
		
		return df;
	}
	
	private Dataset<Row> unionDataframes(Dataset<Row> dfA, Dataset<Row> dfB) {
		Dataset<Row> unionDf = dfA.union(dfB);
		return unionDf;
	}
}
