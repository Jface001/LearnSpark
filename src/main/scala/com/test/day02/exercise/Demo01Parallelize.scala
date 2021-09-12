package com.test.day02.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Date: 2021/8/15 20:25
 * @Desc: 学习RDD加载方式1: 并行化集合优化,
 */
object Demo01Parallelize {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val conf: SparkConf = new SparkConf()
      .setAppName("MapPartitions")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2.创建一个集合
    val linesSeq: Seq[String] = Seq(
      "hello me  you   her",
      "hello you her",
      "hello her",
      "hello"
    )
    //3.加载本地集合到RDD,并行化集合优化并设置RDD分区个数
    val inputRDD: RDD[String] = sc.parallelize(linesSeq, 2)
    //4.WordCount并遍历输出
    val outputRDD: RDD[(String, Int)] = inputRDD.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
    outputRDD.foreach(println(_))
    //5.关闭上下文对象
    sc.stop()
  }

}
