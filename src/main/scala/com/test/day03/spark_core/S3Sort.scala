package com.test.day03.spark_core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: Jface
 * @Date: 2021/8/17 10:22
 * @Desc: 学习 Spark-core中的 sort 函数
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
    //每个单词标记为1
    val tupleRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    val wordCountRDD: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    //sortByKey
    wordCountRDD.map(x=>x._2->x._1).sortByKey(false).take(3).foreach(println(_))
    println("---我是分割线---")
    //sortBy
    wordCountRDD.sortBy(x=>x._2,false).take(3).foreach(println(_))
    println("---我是分割线---")
    //top
    wordCountRDD.top(3)(Ordering.by(x=>x._2)).foreach(println(_))

    //关闭上下文对象
    sc.stop()

  }

}
