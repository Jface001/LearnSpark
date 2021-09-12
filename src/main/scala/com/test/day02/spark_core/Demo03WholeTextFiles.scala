package com.test.day02.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Date: 2021/8/15 15:08
 * @Desc:
 */
object Demo03WholeTextFiles {

  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //使用wholetextfiles加载文本形成RDD
    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles("src/main/data/words.txt", 2)
    //打印RDD
    //inputRDD.foreach(println(_))
    //获取分区数
    //println("分区数是:"+inputRDD.getNumPartitions)
    //统计RDD有多少行
    println("总共行数为:"+inputRDD.map(_._2).flatMap(_.split("\n")).count())
    //关闭上下文
    sc.stop()
  }

}
