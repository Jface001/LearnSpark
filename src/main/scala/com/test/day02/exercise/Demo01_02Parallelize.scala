package com.test.day02.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc: 学习RDD加载方式1: 并行化集合优化,
 */
object Demo01_02Parallelize {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val conf: SparkConf = new SparkConf()
      .setAppName("MapPartitions")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2.创建一个集合
    val list: List[Int] = (1 to 10).toList
    //3.加载本地集合到RDD,并行化集合优化并设置RDD分区个数
    val inputRDD: RDD[Int] = sc.parallelize(list, 2)
    //4.求和并遍历输出
    val sum: Int = inputRDD.reduce(_ + _)
    println(sum)
    //5.关闭上下文对象
    sc.stop()
  }

}
