package com.test.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Date: 2021/8/14 19:42
 * @Desc: Spark应用开发-本地实现WordCount
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    //2.加载文本文件words.txt,生成一个RDD
    val inputRDD: RDD[String] = sc.textFile("src/main/data/words.txt")
    //3.对RRD进行扁平化成单词
    val flatRDD = inputRDD.flatMap((x) => {
      x.split(" ")
    })
    //4.继续对每个单词标记为1
    val wordOneRDD = flatRDD.map((_, 1))
    //5继续reduceByKey进行分组统计
    val ouputRDD = wordOneRDD.reduceByKey(_ + _)
    //6.生成最后的RDD, 将结果打印到控制台
    ouputRDD.foreach(println(_))
    //7.关闭上下文
    sc.stop()


  }


}
