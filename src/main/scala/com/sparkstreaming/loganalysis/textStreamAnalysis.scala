package com.sparkstreaming.loganalysis

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j._


class addStreaming(sc: SparkContext, sqlContext: SQLContext, cpDir: String) {

  def creatingFunc(): StreamingContext = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val batchInterval = Seconds(10)
    val ssc = new StreamingContext(sc, batchInterval)

    // Set the active SQLContext so that we can access it statically within the foreachRDD
    SQLContext.setActive(sqlContext)

    ssc.checkpoint(cpDir)

    val streamRDD = ssc.textFileStream("/home/dataflair/SparkProjects/SparkStreamingProject/src/main/resources/")

    val resultRDD = streamRDD
      .map(scala.xml.XML.loadString _).map(x => {
        val dt = (x \ "RECORD" \ "DATE").text
        val host = (x \ "RECORD" \ "HOST").text
        val ip = (x \ "RECORD" \ "IP").text
        (dt, host, ip)
      })

    resultRDD.foreachRDD(row => {
      val spark = SparkSession.builder.config(row.sparkContext.getConf)
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .getOrCreate()
      println(spark)
      import spark.implicits._
      import org.elasticsearch.spark.sql._
      
      val streamDF = row.toDF("DATE", "HOST", "IP")

      streamDF.write
      .format("org.elasticsearch.spark.sql").option("es.port", "9200")
      .option("es.nodes", "localhost")
      .mode("append")
      .save("/streamingdata/docs")
      
    })

    ssc
  }
}

object textStreamAnalysis {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val cpDir = "/tmp/checkpoint"
    val conf = new SparkConf().setMaster("local[*]").setAppName("textSTream")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val addStr = new addStreaming(sc, sqlContext, cpDir)
    val xsc = StreamingContext.getActiveOrCreate(cpDir, addStr.creatingFunc _)
    xsc.start()
    xsc.awaitTermination()
  }
}


//export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"