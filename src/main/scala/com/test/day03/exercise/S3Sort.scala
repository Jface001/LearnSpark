package com.test.day03.exercise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: Jface
 * @Date: 2021/8/17 12:16
 * @Desc: 演示 Spark 中排序相关的函数使用
 */
object S3Sort {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //加载数据形成RDD
    val linesSeq: Seq[String] = Seq(
      "hello me you her",
      "hello you her",
      "hello her",
      "hello"
    )
    val inputRDD: RDD[String] = sc.parallelize(linesSeq)
    //将RDD的长文本拆分成单词
    val flatRDD: RDD[String] = inputRDD.flatMap(_.split("\\s+"))
    //每个单词标记为1,并做统计
    val tupleRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    val wordCountRDD: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)

    //sortBy, 需要先交换位置
    println("sortBy方式")
    wordCountRDD.sortBy(x => x._2, false).take(3).foreach(println(_))

    //sortByKey
    println("sortByKey方式")
    wordCountRDD.map(x => x._2 -> x._1).sortByKey(false).take(3).map(x => x.swap).foreach(println(_))
    //top
    println("top方式")
    wordCountRDD.top(3)(Ordering.by(x => x._2)).foreach(println(_))

    //关闭上下文对象
    sc.stop()
  }

}
