package com.test.day02.exercise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: Jface
 * @Date: 2021/8/15 20:42
 * @Desc: Spark 创建RDD之 加载多个小文件
 */
object Demo03WholeTextFile {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象, 将当成类名设置成job名
    val conf: SparkConf = new SparkConf()
      .setAppName(getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2.加载本地文本文件, key是文件路径, value是文件内容, 建议的最小分区个数
    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles("src/main/data/input/ratings10", 2)
    //测试一下
    inputRDD.foreach(println(_))
    //分区数
    println("分区个数是:" + inputRDD.getNumPartitions)
    //3.进行WordCount并遍历输出
    val flatRDD: RDD[String] = inputRDD.flatMap((x) => {
      x._2.split("\\s+")
    })
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    val outputRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    outputRDD.foreach(println(_))

    //.关闭上下文对象
    sc.stop()


  }

}
