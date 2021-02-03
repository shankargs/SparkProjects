package com.sparkstreaming.loganalysis

import org.apache.spark.sql.SparkSession
import scala.xml.XML
import org.apache.spark.sql.types._

object streamingSpark {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("Streaming only with sparksession")
    .master("local[*]").getOrCreate()
    
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("OFF")
//    spark.sparkContext.setCheckpointDir("/tmp/checkpoint")
    
    val schema = StructType(List(StructField("API_VER", StringType),
       StructField("APP_CONTEXT", StringType),
       StructField("CATEGORY", StringType),
       StructField("DATE", StringType),
       StructField("E2EDATA", StringType),
       StructField("EPOCH", DoubleType),
       StructField("HOST", StringType),
       StructField("IP", StringType),
       StructField("LOCATION", StringType),
       StructField("MESSAGE_ID", StringType),
       StructField("PORT", StringType),
       StructField("RECORD_VER", FloatType),
       StructField("SERVER", StringType),
       StructField("SERVERITY", StringType),
       StructField("TEXT", StringType),
       StructField("TIER", StringType),
       StructField("TIME", StringType)))
    
    val streamDF = spark.readStream
    .option("path", "/home/dataflair/SparkProjects/SparkStreamingProject/src/main/resources")
    .format("text")
//    .schema(schema)
//    .option("rootTag", "RECORDS")
//    .option("rowTag", "RECORD")
    .load()
    
   streamDF.selectExpr("xpath(value, 'RECORDS/RECORD/HOST/text()')", "xpath(value, 'RECORDS/RECORD/IP/text()')", "xpath(value, 'RECORDS/RECORD/E@EDATA/text()')")
   .writeStream.format("console").start().awaitTermination()
    
    
  }
}