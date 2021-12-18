package com.test.day03.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc: 学习 PairRDDFunctions 的使用
 */
object S1PairRDDFunctions {
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
    val outputRDD2: RDD[(String, Int)] = groupedRDD.mapValues(_.sum)
    val outputRDD1: RDD[(String, Int)] = groupedRDD.map((x) => {
      (x._1 -> x._2.sum)
    })

    //reduceByKey 和foldByKey
    //先ByKey hello->[1,1,1,1]
    //再Reduce 聚合_+_ 最后编程 hello->4
    val outputRDD3: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    val outputRDD4: RDD[(String, Int)] = tupleRDD.foldByKey(0)(_ + _)
    //aggregateByKey
    val outputRDD5: RDD[(String, Int)] = tupleRDD.aggregateByKey(0)(_ + _, _ + _)
    //打印结果
    println("groupByKey结果1")
    outputRDD1.foreach(println(_))
    println("groupByKey结果2")
    outputRDD2.foreach(println(_))
    println("reduceByKey结果")
    outputRDD3.foreach(println(_))
    println("fold结果")
    outputRDD4.foreach(println(_))
    println("aggregateByKy结果")
    outputRDD5.foreach(println(_))

    //关闭上下文
    sc.stop()
  }

}
