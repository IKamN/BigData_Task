package org.example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions.{col, current_timestamp, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, StreamingContext}

object Accident extends App {

  //Parameters
  val kafkaTopic = Array("accident")
  val server_kafka = "localhost:9092"

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Minutes(1))
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
      val first_row = spark.sparkContext.parallelize(RDD_value.take(1))
      var res = RDD_value.subtract(first_row)

      val split_string = res.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
      var filter_data = split_string.filter(row => row.length == 29)
      var data_row = filter_data.map(y => Row(y(0), y(1), y(2), y(3), y(4), y(5), y(6), y(7),
            y(8), y(9), y(10), y(11), y(12), y(13), y(14), y(15), y(16), y(17), y(18),
            y(19), y(20), y(21), y(22), y(23), y(24), y(25), y(26), y(27), y(28)))

      var header = RDD_value.first().split(",").map(_.replace(" ", "_"))
      val fields = header.map(fieldName => StructField(fieldName, StringType, nullable = true))
      val schemaAccident = StructType(fields)

      var data = spark.sqlContext.createDataFrame(data_row, schemaAccident)

      data.createOrReplaceTempView("Injured")
      val injured_q =
        """
          |select
          |current_timestamp()
          |,sum(NUMBER_OF_PERSONS_INJURED) as count_injured
          |,sum(NUMBER_OF_PEDESTRIANS_INJURED) as count_pedestrians_injured
          |,sum(NUMBER_OF_CYCLIST_INJURED) as count_cyclist_injured
          |,sum(NUMBER_OF_MOTORIST_INJURED) as count_motorist_injured
          |from
          | Injured;
          |""".stripMargin

      data = data.withColumn("ZIP_CODE", when(col("ZIP_CODE")
        .equalTo(""), null).otherwise(col("ZIP_CODE")))
        .withColumn("BOROUGH", when(col("BOROUGH")
        .equalTo(""), null).otherwise(col("BOROUGH")))
      var new_df = data.na.drop(Seq("ZIP_CODE", "BOROUGH"))
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
      //injured_df.show()
      val rating_zip_code_df = spark.sql(rating_zip_code_q)
      //rating_zip_code_df.show()
      val rating_borough_df = spark.sql(rating_borough_q)
      //rating_borough_df.show()

      injured_df.write
        .mode("overwrite")
        .format("jdbc")
        .option("url", "jdbc:postgresql://172.20.0.2:5432/db_accident")
        .option("dbtable", "injured")
        .option("user", "postgres")
        .option("password", "7627")
        .save()

      rating_zip_code_df.write
        .mode("overwrite")
        .format("jdbc")
        .option("url", "jdbc:postgresql://172.20.0.2:5432/db_accident")
        .option("dbtable", "rating_zip_code")
        .option("user", "postgres")
        .option("password", "7627")
        .save()

      rating_borough_df.write
        .mode("overwrite")
        .format("jdbc")
        .option("url", "jdbc:postgresql://172.20.0.2:5432/db_accident")
        .option("dbtable", "rating_borough")
        .option("user", "postgres")
        .option("password", "7627")
        .save()

      // Write partition subfolder
      data = data.withColumn("current_timestamp", current_timestamp())
      data.write.option("header", "true")
        .partitionBy("current_timestamp")
        .mode("overwrite")
        .csv("/home/kini/Accident")
  }

  ssc.start()
  ssc.awaitTerminationOrTimeout(timeout = 2000000)
}