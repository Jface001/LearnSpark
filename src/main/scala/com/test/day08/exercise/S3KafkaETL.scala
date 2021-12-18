package com.test.day08.exercise

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * @Author: Jface
 * @Desc: 模拟消费者, 消费 Kafka的数据, 并输出到 Kafka
 */
object S3KafkaETL {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    //2.加载 Kafka 的数据源, 读取默认主题数据, 返回流式 DataFrame
    val inputDF: DataFrame = spark.readStream
      .format("kafka")
      //指定 kafka 集群地址
      .option("kafka.bootstrap.servers", "node1:9092")
      //指定消费主题
      .option("subscribe", "stationTopic")
      .load()

    //3.将DataFrame 转为 DataSet
    import spark.implicits._
    val inputDS: Dataset[String] = inputDF.as[String]
    //4.对 DataSet 做清洗转换 形成结果DataFrame
    val resultDS: Dataset[String] = inputDS.filter(x => {
      StringUtils.isNotBlank(x) && x.split(",").length == 6
    })
      .filter(x => {
        val arr: Array[String] = x.split(",")
        "success".equals(arr(3))
      })
    //5.将结果 DataFrame 保存的 Kafka 中
    val query: StreamingQuery = resultDS.writeStream
      .format("kafka")
      .outputMode(OutputMode.Append())
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("checkpointLocation", "./" + this.getClass.getSimpleName.stripSuffix("$"))
      .option("topic", "etlTopic")
      .start()
    //6.启动停留关闭
    query.awaitTermination()
    query.stop()
  }

}
