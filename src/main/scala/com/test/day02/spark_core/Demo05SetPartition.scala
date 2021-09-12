package com.test.day02.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}

/**
 * @Author: Jface
 * @Date: 2021/8/15 14:59
 * @Desc:
 */
object Demo05SetPartition {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    //使用textfile加载文本形成RDD
    val inputRDD = sc.textFile("src/main/data/words2.txt")
    //分区数增大为8
    val partRDD: RDD[String] = inputRDD.repartition(8)
    println("RDD的分区数是: " + partRDD.getNumPartitions)
    //对RDD做wordcount
    val mapRDD = partRDD.flatMap(_.split("\\s")).map((_, 1))
    //缩小为3个分区, 大写字母进入1分区, 小写字母0分区, 其它字符2分区
    val partByRDD: RDD[(String, Int)] = mapRDD.partitionBy(new MyPartitioner)
    partByRDD.foreachPartition(par => {
      par.foreach(x => {
        val num: Int = TaskContext.getPartitionId()
        println("分区号是:" + num + ", 元素是:" + x)
      })
    })

    val outputRDD: RDD[(String, Int)] = partByRDD.reduceByKey(_ + _)
    //打印结果, 将结果减少为1个分区数, 并写到文件, RDD有几个分区, 文件就有几个
    outputRDD.coalesce(1).saveAsTextFile("src/main/data/output/")
    //关闭上下文
    sc.stop()
  }

  //自定义的分区器, 继承Partitioner类, 自定义key的分区规则
  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      val firstChar = key.asInstanceOf[String].charAt(0).toInt
      if (firstChar >= 97 && firstChar <= 122) {
        0
      }
      else if (firstChar >= 65 && firstChar <= 90) {
        1
      }
      else {
        2
      }
    }
  }


}
