package com.test.day02.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Date: 2021/8/15 14:59
 * @Desc:
 */
object Demo04Partition {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //使用textfile加载文本形成RDD
    val inputRDD = sc.textFile("src/main/data/words.txt", 2)
    //对RDD做wordcount
    //val outputRDD = inputRDD.flatMap(_.split("\\s")).map((_, 1)).reduceByKey(_ + _)
    //打印结果
    //outputRDD.foreach(println(_))

    //用mapPartitions 代替Map
    val flatRDD: RDD[String] = inputRDD.flatMap(_.split("\\s"))
    val mapRDD: RDD[(String, Int)] = flatRDD.mapPartitions(par => {
      val tmp = par.map((_, 1))
      tmp
    })
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    //用foreachPartiton 代替foreach
    reduceRDD.foreachPartition(par => {
      val result: Unit = par.foreach(println(_))
      result
    })


    //关闭上下文
    sc.stop()
  }

}
