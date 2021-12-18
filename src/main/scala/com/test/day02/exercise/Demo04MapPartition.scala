package com.test.day02.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc: 学习Spark分区操作函数
 */
object Demo04MapPartition {
  def main(args: Array[String]): Unit = {
    //1.新建上下文对象
    val conf: SparkConf = new SparkConf()
      .setAppName("MapPartitions")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2.加载文件文本文件
    val inputRDD: RDD[String] = sc.textFile("src/main/data/words.txt")
    //3.对RDD进行扁平化操作, 获取到每个单词
    val flatRDD: RDD[String] = inputRDD.flatMap(_.split("\\s"))
    //4.对分区进行mapPartitions操作,变成key-value结构
    val mapRDD: RDD[(String, Int)] = flatRDD.mapPartitions(par => {
      val par2: Iterator[(String, Int)] = par.map((_, 1))
      par2
    })
    //5.分组数据求和
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    //6.foreachPartition 遍历输出
    reduceRDD.foreachPartition(par => {
      par.foreach(println(_))
    })
    //7.关闭上下文对象
    sc.stop()

  }


}
