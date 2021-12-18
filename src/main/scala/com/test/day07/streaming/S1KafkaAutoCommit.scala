package com.test.day07.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Set

/**
 * @Author: Jface
 * @Desc: Spark  Kafka自动提交offset
 */
object S1KafkaAutoCommit {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //准备kafka连接参数
    val kafkaParams = Map(
      "bootstrap.servers" -> "node1:9092,node2:9092,nodo3:9092",
      "key.deserializer" -> classOf[StringDeserializer], //key的反序列化规则
      "value.deserializer" -> classOf[StringDeserializer], //value的反序列化规则
      "group.id" -> "spark", //消费者组名称
      //earliest:表示如果有offset记录从offset记录开始消费,如果没有从最早的消息开始消费
      //latest:表示如果有offset记录从offset记录开始消费,如果没有从最后/最新的消息开始消费
      //none:表示如果有offset记录从offset记录开始消费,如果没有就报错
      "auto.offset.reset" -> "latest", //offset重置位置
      "auto.commit.interval.ms" -> "1000", //自动提交的时间间隔
      "enable.auto.commit" -> (true: java.lang.Boolean) //是否自动提交偏移量到kafka的专门存储偏移量的默认topic
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("spark_kafka"), kafkaParams)
    )
    //连接kafka, 拉取一批数据, 得到DSteam
    val resutDStream: DStream[Unit] = kafkaDStream.map(x => {
      println(s"topic=${x.topic()},partition=${x.partition()},offset=${x.offset()},key=${x.key()},value=${x.value()}")
    })
    //打印数据
    resutDStream.print()
    //启动并停留
    ssc.start()
    ssc.awaitTermination()
    //合理化关闭
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
