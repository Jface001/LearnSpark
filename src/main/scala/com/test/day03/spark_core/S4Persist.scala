package com.test.day03.spark_core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Date: 2021/8/17 11:10
 * @Desc: 将文本内存持久化到磁盘或内存, 并统计行数
 */
object S4Persist {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //加载文本形成RDD
    val inputRDD = sc.textFile("src/main/data/words.txt",2)
    //将RDD持久化,三个等价
    inputRDD.persist()
    //inputRDD.cache()
    //inputRDD.persist(StorageLevel.MEMORY_ONLY)

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
