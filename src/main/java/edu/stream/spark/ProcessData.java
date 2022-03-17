package edu.stream.spark;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.TimeoutException;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructType;

import scala.Function1;
import scala.reflect.ClassTag;

public class ProcessData {
	public static void main(String args[]) throws StreamingQueryException, TimeoutException {
		//System.setProperty("hadoop.home.dir", "./hadoop/");
		WeatherDataExtractor dataGenerator = new WeatherDataExtractor(args[0]);
		Thread t1 = new Thread(dataGenerator);
		t1.start();
		Integer aggregate_period_sec = null;
		try {
			
			aggregate_period_sec = Integer
					.parseInt(dataGenerator.loadProperties(args[0], "aggregate_period_sec").trim());
		} catch (IOException e2) {
			System.out.println("Unable to read config file");
			System.exit(0);
		}

		StructType weatherSchema = new StructType().add("datetime", "timestamp").add("lon", "decimal")
				.add("lat", "decimal").add("weather_id", "integer").add("weather_main", "string")
				.add("description", "string").add("icon", "string").add("base", "string").add("temp", "decimal")
				.add("pressure", "decimal").add("humidity", "decimal").add("temp_min", "decimal")
				.add("temp_max", "decimal").add("visibility", "decimal").add("wind_speed", "decimal")
				.add("wind_deg", "decimal").add("clouds_all", "decimal").add("dt", "long").add("sys_id", "integer")
				.add("sys_type", "integer").add("country", "string").add("sunrise", "long").add("sunset", "long")
				.add("id", "long").add("name", "string").add("cod", "string");
		SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkKafkaStreamingJob").getOrCreate();
		spark.udf().register("getFrequentDes", new UDF1<String, String>() {
			  @Override
			  public String call(String wordsStr) {
				  ArrayList<String> words = new ArrayList<String>();
					words.addAll(Arrays.asList(wordsStr.toString().split(","))); 
					String word ="";
					int count = 0, maxCount = 0;

							// Determine the most repeated word in a file 
					for (int i = 0; i <words.size(); i++) { 
						count = 1; // Count each word in the file and store it in variable count 
							for (int j = i + 1; j < words.size(); j++) { 
								if(words.get(i).equals(words.get(j)))
								{ 
									count++; 
								} 
							} 
							// If maxCount is less than count then store value of count in maxCount // and corresponding word to variable word 
							if (count > maxCount) { 
								maxCount = count; word = words.get(i);
							} 
					}

					return word; 
			  }
			}, DataTypes.StringType);
		
				
		Dataset<Row> dataDF = spark.readStream().option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ").option("sep", ";").schema(weatherSchema)
				.csv(args[1]);


		Dataset<Row> filteredData = dataDF.select(col("name"), col("datetime"),
				col("temp"),col("description")).groupBy(functions.window(col("datetime"),
						aggregate_period_sec + " Seconds"),col("name")).agg(functions.column("name"),functions.avg("temp").alias("avg_temperature"),
								functions.max("temp").alias("_max"),functions.min("temp").alias("_min"),functions.concat_ws(",",
										functions.collect_list("description")).alias("freq_desc"))
				.select(col("name").alias("location"),col("window.start").alias("datetime"),col
						("avg_temperature"), col("_max"), col("_min"),functions.call_udf("getFrequentDes", col("freq_desc")));
		
	

		
		  Dataset<Row> streamingTaskResult_1
		  =filteredData.select(col("location"),col("datetime"),col("avg_temperature"),
		  col("_max").minus(col("_min")).alias("temperature_diff"),col("getFrequentDes(freq_desc)").alias("desc"));
		  streamingTaskResult_1.writeStream().outputMode("complete").format("console").start();
		 
		  	

			/*
			 * Dataset<Row>
			 * dataSet3=streamingTaskResult_1.withColumn("avg_global_temperature",
			 * functions.avg("avg_temperature").over(
			 * Window.partitionBy(col("datetime"))).alias("temp"))
			 * .withColumn("temperature_change",
			 * functions.col("avg_temperature").minus(functions.lag(col("avg_temperature"),
			 * 120).over( Window.partitionBy(col("datetime"))).alias("temp")));
			 * 
			 * dataSet3.writeStream().outputMode("complete").format("console").start();
			 */
			 
			/*
			 * org.apache.spark.sql.AnalysisException: Non-time-based windows are not
			 * supported on streaming DataFrames/Datasets; Window [avg(avg_temperature#62)
			 * windowspecdefinition(datetime#78, specifiedwindowframe(RowFrame,
			 * unboundedpreceding$(), unboundedfollowing$())) AS temp#95], [datetime#78] +-
			 * Project [location#77, datetime#78, avg_temperature#62, temperature_diff#87,
			 * desc#88] +- Project [location#77, datetime#78, avg_temperature#62,
			 * CheckOverflow((promote_precision(cast(_max#64 as decimal(11,0))) -
			 * promote_precision(cast(_min#66 as decimal(11,0)))), DecimalType(11,0), true)
			 * AS temperature_diff#87, getFrequentDes(freq_desc)#80 AS desc#88]
			 */
	}


	

}
