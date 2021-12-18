package com.test.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc: Spark应用开发-Linux上实现WordCount
 */
object WordCount_Linux {
  def main(args: Array[String]): Unit = {
    //0.创建输入路径和输出路径
    val input_path = args(0)
    val output_path = args(1)
    //1.创建上下文对象
    val conf = new SparkConf().setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    //2.加载文本文件words.txt,生成一个RDD
    val inputRDD: RDD[String] = sc.textFile(input_path)
    //3.对RRD进行扁平化成单词
    val flatRDD = inputRDD.flatMap((x) => {
      x.split(" ")
    })
    //4.继续对每个单词标记为1
    val wordOneRDD = flatRDD.map((_, 1))
    //5继续reduceByKey进行分组统计
    val ouputRDD = wordOneRDD.reduceByKey(_ + _)
    //6.生成最后的RDD, 将结果上传到HDFS
    ouputRDD.saveAsTextFile(output_path)
    //7.关闭上下文
    sc.stop()

  }


}
