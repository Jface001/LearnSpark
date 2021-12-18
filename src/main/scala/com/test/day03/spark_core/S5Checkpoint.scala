package com.test.day03.spark_core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc: 将文本内存持久化到HDFS
 */
object S5Checkpoint {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //加载文本形成RDD
    val inputRDD = sc.textFile("src/main/data/words.txt",2)
    //将RDD持久化到 HDFS(模拟)
    //设置目录
    sc.setCheckpointDir("src/main/data/ckp")
    //使用checkpoint
    inputRDD.checkpoint()
    // TODO: 持久化函数需要 Action 函数触发, 通常使用 count
    inputRDD.count()
    //再统计RDD元素个数, 此时会直接读取持久化的数据, 比上面的快
    val cnt: Long = inputRDD.count()
    println(cnt)
    Thread.sleep(5*60*1000)
    //关闭上下文对象
    sc.stop()



  }

}
