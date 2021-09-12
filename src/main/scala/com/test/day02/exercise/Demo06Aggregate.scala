package com.test.day02.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * @Author: Jface
 * @Date: 2021/8/15 18:24
 * @Desc: 练习Spark Core 中没有 Key 值的的聚合函数
 *        aggregate
 *        fold 内嵌两层函数相同的aggregate, 就可以简写成 fold
 *        reduce 初始值为 0 可以简化为 fold
 */
object Demo06Aggregate {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val conf: SparkConf = new SparkConf()
      .setAppName("AggregateTest")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2.并行优化加载集合并设置建议的最小分区数
    val inputRDD: RDD[Int] = sc.parallelize(1 to 10, 2)
    //3.求和
    //方式1 aggregate
    val aggregateSum: Int = inputRDD.aggregate(0)(
      (x, y) => {
        println(s"分区内的分区号=${TaskContext.getPartitionId()},x=${x},y=${y},sum=${x + y}")
        x + y
      },
      (x, y) => {
        println(s"分区间的分区号=${TaskContext.getPartitionId()},x=${x},y=${y},sum=${x + y}")
        x + y
      }

    )
    //方式2 fold
    val foldSum: Int = inputRDD.fold(0)(_ + _)

    //方式3 reduce
    val reduceSum: Int = inputRDD.reduce(_ + _)

    //4.打印结果
    println(aggregateSum)
    println(foldSum)
    println(reduceSum)

    //5.关闭上下文对象
  }

}
