package com.test.day02.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}

/**
 * @Author: Jface
 * @Date: 2021/8/15 19:44
 * @Desc: 学习Spark中设置分区个数方式
 *        repartition 增加分区个数
 *        coalesce 减少分区个数
 *        PartitionBy 设置分区个数, 自定义分区器
 */
object Demo05Partition {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val conf = new SparkConf()
      .setAppName("PartitonTest")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    //2.加载文本文件生成RDD
    val inputRDD: RDD[String] = sc.textFile("src/main/data/words2.txt")
    //3.将分区数设置为8
    val partRDD: RDD[String] = inputRDD.repartition(8)
    println("分区个数是:" + partRDD.getNumPartitions)
    //4.对RDD进行扁平化成单词
    val flatRDD: RDD[String] = partRDD.flatMap(_.split(" "))
    //5.将单词变成 key-value 结构
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    //6.对RDD进行分组统计
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    //7.设置分区个数为3, 大写字母进入0号分区, 小写字母进入1号分区, 其它字符进入2号分区
    val myPartRDD: RDD[(String, Int)] = reduceRDD.partitionBy(new MyPartitioner)
    //测试是否自定义分区成功
    myPartRDD.foreachPartition((x) => {
      x.foreach(x => {
        println(s"分区号是${TaskContext.getPartitionId()},元素是:+${x}")
      })
    })
    //8.设置分区个数为1, 将结果存储到指定路径
    myPartRDD.coalesce(1).saveAsTextFile("src/main/data/output")

    //9.关闭上下文对象

  }

  class MyPartitioner extends Partitioner {
    /**
     * 用于设置分区个数
     *
     * @return
     */
    override def numPartitions: Int = 3

    /**
     * 用于设置分区规则
     *
     * @param key 根据什么分区
     * @return
     */
    override def getPartition(key: Any): Int = {
      //先转化字符串, 再切割获取首字母, 再转成ASCII码表对应数字
      val firstChar: Int = key.asInstanceOf[String].charAt(0).toInt
      if (firstChar >= 65 && firstChar <= 90) {
        0
      }
      else if (firstChar >= 97 && firstChar <= 122) {
        1
      }
      else {
        2
      }
    }
  }


}
