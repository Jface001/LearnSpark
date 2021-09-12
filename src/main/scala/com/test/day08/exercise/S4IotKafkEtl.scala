package com.test.day08.exercise

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/27 9:33
 * @Desc:
 */
object S4IotKafkEtl {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    //2.指定消费 kafka 数据, 获取动态 DataFrame
    val inputDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node3:9092")
      .option("subscribe", "iotTopic")
      .load()
    //3.将 DataFrame 转换成  DataSet
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //4.对 DataSet 清洗转化, 获取结果 DataSet
    //二进制转换成String
    val resulttable: DataFrame = inputDF.selectExpr("case (value as string)")
      .as[String] //转成String的value
      .filter(StringUtils.isNotBlank(_)) //value是String类型才能做非空行判断
      //将每行数据转换成schema信息的数据
      .select(
        get_json_object($"value", "$.device").as("device_id"),
        get_json_object($"value", "$.deviceType").as("device_type"),
        get_json_object($"value", "$.signal").as("signal"),
        get_json_object($"value", "$.time").as("time")
      )

    //5.按照需求统计
    //TODO: 1）、信号强度大于30的设备
    //TODO: 2）、各种设备类型的数量
    //TODO: 3）、各种设备类型的平均信号强度
    //TODO: SQL 风格实现
    //创建临时视图
    resulttable.createOrReplaceTempView("t_iot")
    val result1: DataFrame = spark.sql(
      """
        |select
        |count(1) as cnt,
        |round(avg(signal),2) as avg_signal
        |from t_iot
        |where signal>30
        |group by device_Type
        |""".stripMargin)
    //执行 SQL 语句
    //TODO: DSL 风格实现
    //多个聚合操作, 需要使用agg操作
    val result2: DataFrame = resulttable.groupBy("device_Type")
      .agg(
        count("device_id").as("cnt"),
        round(avg("signal"), 2).as("avg_signal")
      )

    //将结果写到控制台
    val query: StreamingQuery = result2.writeStream
      .format("cosole")
      .outputMode(OutputMode.Complete())
      .option("truncate", false) //全部显示, 默认显示17个字符
      .start()
    //6.启动停留关闭
    query.awaitTermination()
    query.stop()


  }

}
