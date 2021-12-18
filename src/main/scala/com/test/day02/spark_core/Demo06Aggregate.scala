package com.test.day02.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc:
 */
object Demo06Aggregate {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //并行优化集合手动指定分区个数是2
    val partRDD: RDD[Int] = sc.parallelize(1 to 10, 2)
    //用aggregate计算元素之和, 需要设置初始值
    val aggregateValue: Int = partRDD.fold(0)(_ + _)
    //打印结果
    println(aggregateValue)
    //关闭上下文
    sc.stop()
  }

}
