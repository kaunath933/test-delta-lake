package com.knoldus.premonr

import org.apache.spark.sql.{DataFrame,Dataset,SparkSession}


object SparkApp {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

    import sparkSession.implicits._

    sparkSession.sparkContext.setLogLevel("Error")

    val dataFrame: DataFrame = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Constants.KafkaHost)
      .option("subscribe", Constants.KafkaTopic)
      .load()

    val dataSet: Dataset[String] =
      dataFrame.selectExpr(Constants.CastToString).as[String]

    val result: DataFrame = dataSet
      .flatMap(value => value.split("\\s+"))
      .groupByKey(_.toInt + 1)
      .count().withColumnRenamed("count(1)", "counts")

    result.writeStream
      .format(Constants.OutputFormat)
      .outputMode(Constants.OutputMode)
      .option("checkpointLocation", "/home/knoldus/Desktop/test/etl-from-json")
      .start("/home/knoldus/delta/events")
      .awaitTermination()

  }

}
