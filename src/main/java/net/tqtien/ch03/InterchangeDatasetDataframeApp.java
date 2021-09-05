package net.tqtien.ch03;

import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;

import net.tqtien.ch03.model.Book;

public class InterchangeDatasetDataframeApp {
	private SparkSession spark;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		InterchangeDatasetDataframeApp app = new InterchangeDatasetDataframeApp();
		app.start();
		
		Dataset<Row> dfBook = app.buildDataframe();
		Dataset<Book> dsBook = app.toDataset(dfBook);
		
		dsBook.printSchema();
		
		Dataset<Row> dfBook2 = dfBook.toDF();
//		dfBook2 = dfBook2.withColumn("date", concat(
//	            expr("releaseDate.year + 1900"), lit("-"),
//	            expr("releaseDate.month + 1"), lit("-"),
//	            dfBook2.col("releaseDate.date")));
//		dfBook2.printSchema();
	}

	private void start() {
		this.spark = SparkSession.builder()
				.appName("Dataset and Dataframe")
				.master("local")
				.getOrCreate();
	}
	
	private Dataset<Row> buildDataframe() {
		Dataset<Row> df = this.spark.read()
				.format("csv")
				.option("header", "true")
				.load("data/books.csv");
		return df;
	}
	
	private Dataset<Book> toDataset(Dataset<Row> df) {
		return df.map(new BookMapper(), Encoders.bean(Book.class));
	}
	
	private class BookMapper implements MapFunction<Row, Book> {
		private static final long serialVersionUID = 1L;

		public Book call(Row value) throws Exception {
			Book book = new Book();
			book.setId((Integer) value.getAs("id"));
			book.setAuthorId((Integer) value.getAs("authorId"));
			book.setLink((String) value.getAs("link"));
			book.setTitle((String) value.getAs("title"));
			
			String dateString = value.getAs("releaseDate");
			if (dateString != null) {
				SimpleDateFormat format = new SimpleDateFormat("M/d/yy");
				book.setReleaseDate(format.parse(dateString));
			}
			 
			return book;
		}
	}
}
