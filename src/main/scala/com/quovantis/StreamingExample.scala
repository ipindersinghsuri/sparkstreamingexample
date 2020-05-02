package com.quovantis

import org.apache.commons.lang.NotImplementedException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingExample {

  val KAFKA_BOOTSTRAP_SERVERS: String = "localhost:9092"
  val KAFKA_TOPIC: String = "ipsparktest"

  def main(args: Array[String]): Unit = {
    initLogger()

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val stream = getStream("kafka", spark)

    val df = processData(spark, stream)

    val query = writeStream(spark, "console", df)

    query.awaitTermination()
  }

  def initLogger(): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)
  }

  def getStream(streamType: String, spark: SparkSession): DataFrame = {

    streamType match {
      case "socket" => spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
      case "kafka" =>
        val df = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
          .option("subscribe", KAFKA_TOPIC)
          .load()
        df.select("value") // in kafka we get data in values column
      case _ => throw new NotImplementedException(s"streamType: {streamType} not implemented yet.")
    }
  }

  def processData(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df
      .withColumn("split_data", split($"value", " "))
      .withColumn("words", explode($"split_data"))
      .groupBy("words").count().sort(desc("count"))
  }

  def writeStream(spark: SparkSession, streamType: String, df: DataFrame): StreamingQuery = {
    import spark.implicits._

    streamType match {
      case "console" => df.writeStream
        .outputMode("complete")
        .format("console")
        .start()
      case "kafka" => df
        .withColumn("value", to_json(struct($"*"))) // we need to specify the data to send in values col
        .select("value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC + "_out")
        .start()
    }
  }

}
