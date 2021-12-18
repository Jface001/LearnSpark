package com.test.day02.spark_core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc:
 */
object Demo02TextFile {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    //使用textfile加载文本形成RDD
    val inputRDD = sc.textFile("src/main/data/words.txt",2)
    //对RDD做wordcount
    val outputRDD = inputRDD.flatMap(_.split("\\s")).map((_, 1)).reduceByKey(_ + _)
    //打印结果
    outputRDD.foreach(println(_))
    //关闭上下文
    sc.stop()
  }

}
