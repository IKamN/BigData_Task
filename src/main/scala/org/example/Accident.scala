package org.example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, StreamingContext}

import scala.util.Try

object Accident extends App {

  //Parameters
  val kafkaTopic = Array("accident")
  val server_kafka = "localhost:9092"

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Minutes(1))
  ssc.sparkContext.setLogLevel("ERROR")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> server_kafka,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test_group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )



  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](kafkaTopic, kafkaParams)
  )

  import spark.implicits._

  stream.foreachRDD {
    rdd =>
      val RDD_value = rdd.map(record => record.value)
      val first_row = sc.parallelize(RDD_value.take(1))
      val res = RDD_value.subtract(first_row)

      val split_string = res.map(_.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1))

      val v = split_string.map(y => Row(
        Try(y(0)) getOrElse("null"),
        Try(y(1)) getOrElse("null"),
        Try(y(2)) getOrElse("null"),
        Try(y(3)) getOrElse("null"),
        Try(y(4)) getOrElse("null"),
        Try(y(5)) getOrElse("null"),
        Try(y(6)) getOrElse("null"),
        Try(y(7)) getOrElse("null"),
        Try(y(8)) getOrElse("null"),
        Try(y(9)) getOrElse("null"),
        Try(y(10)) getOrElse("null"),
        Try(y(11)) getOrElse("null"),
        Try(y(12)) getOrElse("null"),
        Try(y(13)) getOrElse("null"),
        Try(y(14)) getOrElse("null"),
        Try(y(15)) getOrElse("null"),
        Try(y(16)) getOrElse("null"),
        Try(y(17)) getOrElse("null"),
        Try(y(18)) getOrElse("null"),
        Try(y(19)) getOrElse("null"),
        Try(y(20)) getOrElse("null"),
        Try(y(21)) getOrElse("null"),
        Try(y(22)) getOrElse("null"),
        Try(y(23)) getOrElse("null"),
        Try(y(24)) getOrElse("null"),
        Try(y(25)) getOrElse("null"),
        Try(y(26)) getOrElse("null"),
        Try(y(27)) getOrElse("null"),
        Try(y(28)) getOrElse("null")
      ))

      val header = RDD_value.first().split(";")
      val fields = header.map(fieldName => StructField(fieldName, StringType, nullable = true))
      val schemaAccident = StructType(fields)
      var data = spark.sqlContext.createDataFrame(v, schemaAccident)

      data = data.withColumnRenamed("NUMBER OF PERSONS INJURED", "NUMBER_OF_PERSONS_INJURED")
        .withColumnRenamed("NUMBER OF PEDESTRIANS INJURED", "NUMBER_OF_PEDESTRIANS_INJURED")
        .withColumnRenamed("NUMBER OF CYCLIST INJURED", "NUMBER_OF_CYCLIST_INJURED")
        .withColumnRenamed("NUMBER OF MOTORIST INJURED", "NUMBER_OF_MOTORIST_INJURED")
        .withColumnRenamed("ZIP CODE", "ZIP_CODE")


      data.createOrReplaceTempView("Injured")
      val injured_q =
        """
          |select
          |current_timestamp()
          |,sum(NUMBER_OF_PERSONS_INJURED)  as count_injured
          |,sum(NUMBER_OF_PEDESTRIANS_INJURED) as count_pedestrians_injured
          |,sum(NUMBER_OF_CYCLIST_INJURED) as count_cyclist_injured
          |,sum(NUMBER_OF_MOTORIST_INJURED) as count_motorist_injured
          |from
          | Injured;
          |""".stripMargin

      val new_df = data.na.drop(Seq("ZIP_CODE", "BOROUGH"))
      new_df.createOrReplaceTempView("rating_zip_code")

      val rating_zip_code_q =
        """
          |select
          |current_timestamp()
          |,RANK() over (order by count(*) desc) as rank
          |,ZIP_CODE
          |,count(*) as count_crashes
          |from
          |rating_zip_code
          |group by
          |ZIP_CODE
          |order by
          |count_crashes desc
          |limit 20;
          |""".stripMargin

      new_df.createOrReplaceTempView("rating_borough")

      val rating_borough_q =
        """
          |select
          |current_timestamp()
          |,RANK() over (order by count(*) desc) as rank
          |,BOROUGH
          |,count(*) as count_crashes
          |from
          |rating_borough
          |group by
          |BOROUGH
        order by
          | count_crashes desc
        limit 10;
          |""".stripMargin

      val injured_df = spark.sql(injured_q)
      injured_df.show()
      val rating_zip_code_df = spark.sql(rating_zip_code_q)
      rating_zip_code_df.show()
      val rating_borough_df = spark.sql(rating_borough_q)
      rating_borough_df.show()

      injured_df.write
        .mode("overwrite")
        .format("jdbc")
        .option("url", "jdbc:postgresql://172.20.0.3:5432/db_accident")
        .option("dbtable", "injured")
        .option("user", "postgres")
        .option("password", "7627")
        .save()

      rating_zip_code_df.write
        .mode("overwrite")
        .format("jdbc")
        .option("url", "jdbc:postgresql://172.20.0.3:5432/db_accident")
        .option("dbtable", "rating_zip_code")
        .option("user", "postgres")
        .option("password", "7627")
        .save()

      rating_borough_df.write
        .mode("overwrite")
        .format("jdbc")
        .option("url", "jdbc:postgresql://172.20.0.3:5432/db_accident")
        .option("dbtable", "rating_borough")
        .option("user", "postgres")
        .option("password", "7627")
        .save()

      // Write partition subfolder
      data = data.withColumn("current_timestamp", current_timestamp())
      data.write.option("header", true)
        .partitionBy("current_timestamp")
        .mode("overwrite")
        .csv("/home/kini/Accident")
  }

  ssc.start()
  ssc.awaitTerminationOrTimeout(timeout = 2000000)

}
