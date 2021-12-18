package com.test.day07.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc: Spark  Kafka 手动提交 offset 到默认 topic
 */
object S2KafkaCommit {
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
      "enable.auto.commit" -> (false: java.lang.Boolean) //是否自动提交偏移量到kafka的专门存储偏移量的默认topic
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("spark_kafka"), kafkaParams)
    )
    //连接kafka, 拉取一批数据, 得到DSteam
    kafkaDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //对每个批次进行处理
        //提取并打印偏移量范围信息
        val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
        val offsetRanges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
        println("它的行偏移量是: ")
        offsetRanges.foreach(println(_))
        //打印每个批次的具体信息
        rdd.foreach(x => {
          println(s"topic=${x.topic()},partition=${x.partition()},offset=${x.offset()},key=${x.key()},value=${x.value()}")
        })
        //手动将偏移量访问信息提交到默认主题
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println("成功提交了偏移量信息")
      }
    })

    //启动并停留
    ssc.start()
    ssc.awaitTermination()
    //合理化关闭
    ssc.stop(true,   true)


  }

}
