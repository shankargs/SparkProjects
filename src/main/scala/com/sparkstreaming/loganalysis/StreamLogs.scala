package com.sparkstreaming.loganalysis

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.sql.types._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamLogs {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Spark Streaming Project").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val schema = StructType(List(
      StructField("lsoa_code", StringType, true),
      StructField("borough", StringType, true),
      StructField("major_category", StringType, true),
      StructField("minor_category", StringType, true),
      StructField("value", IntegerType, true),
      StructField("year", IntegerType, true),
      StructField("month", IntegerType, true)))

    val streamDF = spark.readStream
      .format("csv")
      .options(Map("path" -> "/home/dataflair/SparkProjects/SparkStreamingProject/src/main/resources", "header" -> "true"))
      .schema(schema)
      .load()

    val resultDF = streamDF.withColumn("DateTime", current_timestamp())

    spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
    spark.conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    spark.conf.set("checkpoint", "/home/dataflair/SparkProjects/SparkStreamingProject/src/main/resources")

    val convictionsPerBorough = resultDF.groupBy("DateTime").sum("value")
      .withColumnRenamed("sum(value)", "convictions")
      .orderBy(col("convictions").desc)

//    convictionsPerBorough.writeStream
//      .format("console").outputMode("complete")
//      .option("truncate", false)
//      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .start().awaitTermination()

    val windowFunctionDF = resultDF
    .groupBy(window($"DateTime", "30 seconds", "18 seconds"))
    .sum("value").withColumnRenamed("sum(value)", "convictions") 
    .orderBy(col("convictions").desc)
      
      windowFunctionDF.writeStream
      .format("console").outputMode("complete")
      .option("truncate", false)
//      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start().awaitTermination()
    
  }

}