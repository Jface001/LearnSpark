package com.test.day02.exercise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: Jface
 * @Desc: 测试Spark创建RDD, 从外部存储系统获取数据集
 */
object Demo02TextFile {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val conf: SparkConf = new SparkConf()
      .setAppName("MapPartitions")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2.加载本地文本文件,建议的最小分区个数
    val inputRDD: RDD[String] = sc.textFile("src/main/data/words.txt",2)
    //4.WordCount并遍历输出
    val outputRDD: RDD[(String, Int)] = inputRDD.flatMap(_.split("\\s")).map((_, 1)).reduceByKey(_ + _)
    outputRDD.foreach(println(_))
    println(getClass.getSimpleName.stripSuffix("$"))
    //5.关闭上下文对象
    sc.stop()


  }

}
