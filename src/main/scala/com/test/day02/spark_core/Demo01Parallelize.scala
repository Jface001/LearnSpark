package com.test.day02.spark_core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc:
 */
object Demo01Parallelize {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    //创建一个Scala集合
    val linesSeq: Seq[String] = Seq(
      "hello me you her",
      "hello you her",
      "hello her",
      "hello"
    )

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARn")
    //加载Scala白底集合List得到RDD
    val inputRDD = sc.parallelize(linesSeq, 2)
    //对RDD进行WordCount操作
    val outputRDD = inputRDD.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
    //打印结果
    outputRDD.foreach(println(_))
    //关闭上下文
    sc.stop()
  }

}
