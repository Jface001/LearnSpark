package com.test.day03.exercise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: Jface
 * @Desc: 学习 Spark 中 K-V 结构的聚合函数
 */
object S1PairFunctions {

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
    //groupByKey
    val groupedRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
    val outputRDD1: RDD[(String, Int)] = groupedRDD.map(x => (x._1 -> x._2.sum))
    val outputRDD2: RDD[(String, Int)] = groupedRDD.mapValues(_.sum)
    //foldByKey
    val outputRDD3: RDD[(String, Int)] = tupleRDD.foldByKey(0)(_ + _)
    //reduceByKey
    val outputRDD4: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    //aggregateByKey
    val outputRDD5: RDD[(String, Int)] = tupleRDD.aggregateByKey(0)(_ + _, _ + _)
    //打印结果
    println("groupByKey结果1是:")
    outputRDD1.foreach(println(_))
    println("groupByKey结果2是:")
    outputRDD2.foreach(println(_))
    println("foldByKey结果是:")
    outputRDD3.foreach(println(_))
    println("reduceByKey结果是:")
    outputRDD4.foreach(println(_))
    println("aggregateByKey结果是:")
    outputRDD5.foreach(println(_))

    //关闭上下文对象
    sc stop()

  }

}
