package Spark_Streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.functions.{col, explode, regexp_replace}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Streaming {
  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts","True")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.host", "9042")

    System.setProperty("hadoop.home.dir", "c:/hadoop")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.host", "9042")
      .getOrCreate()

    import spark.implicits._
    val sqlContext = new SQLContext(sc)

    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("customer")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val stream1 = stream.map(record => (record.key, record.value))
    //stream1.print()



    stream1.foreachRDD( rddRaw => if(!rddRaw.isEmpty())
    {

      val data = spark.read.json()
      val withoutid = data.withColumn("results",explode(col("results"))).select("results.user.username")
        .withColumn("username", regexp_replace(col("username"), "([0-9])", ""))
      withoutid.show()
      //rddRaw.collect().foreach(f => { println(f)})
      rddRaw.take(10).foreach(println)





    }
    )






    ssc.start()
    ssc.awaitTermination()

  }


}
