package com.test.day03.sougouQ

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

/**
 * @Author: Jface
 * @Date: 2021/8/17 20:24
 * @Desc: 实现日志数据分析的入口
 */
object SougouQAnalyse {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.从文件中读取数据
    val inputRDD: RDD[String] = sc.textFile("src/main/data/input/SogouQ.sample")
    //3.解析每一条并存入样例类对象
    //过滤空行和不完整的数据
    val recordRDD: RDD[SougouRecord] = inputRDD.filter(x => {
      StringUtils.isNotBlank(x) && x.split("\\s+").length == 6
    })
      //切割获取对应字段
      .map((x) => {
        val arr: Array[String] = x.split("\\s+")

        //把字段放入样例类对象
        val Array(querytime,
        userId,
        queryWords,
        resultRank,
        clickRank,
        clickUrl) = arr
        //搜索关键字字段需要切割前后字符
        SougouRecord(querytime, userId, queryWords.stripSuffix("]").stripPrefix("["), resultRank.toInt, clickRank.toInt, clickUrl)
      })
    //持久化recordRDD
    recordRDD.persist(StorageLevel.MEMORY_AND_DISK_2)
    //4.TODO: 搜索关键词统计
    println("搜索关键词统计:")
    //4.1 获取对象中的 queryWords 字段
    val wordRDD: RDD[String] = recordRDD.flatMap(x => {
      val words: String = x.queryWords
      //4.2 对每个 queryWords 拆分成每个单词
      val term: mutable.Buffer[Term] = HanLP.segment(words).asScala
      term.map(_.word)
    })
    //4.3 对单词进行 WordCount
    val wordcountRDD: RDD[(String, Int)] = wordRDD.map((_, 1)).reduceByKey(_ + _)
    //4.4 取词频 Top10
    wordcountRDD.sortBy(_._2, false).take(10).foreach(println(_))

    //5.TODO: 用户搜索点击统计
    println("用户搜索点击统计:")
    //5.1 将 用户id 和搜索的单词合并为复合对象
    val cntRDD: RDD[(String, Int)] = recordRDD.map((x) => {
      val id: String = x.userId
      val words: String = x.queryWords
      val str: String = id + "_" + words
      str
    })
      //5.2 对复合对象统计次数
      .map((_, 1))
      //5.3 对单词进行 WordCount
      .reduceByKey(_ + _)
    //5.4 取词频 Top10
    //val TopRDD: Array[(String, Int)] = cntRDD.sortBy(_._2, false).take(10)
    //5.5 求最大次数, 最小次数, 平均次数
    val tmpRDD: RDD[Int] = cntRDD.map(x => x._2)
    println("最大值是:" + tmpRDD.max())
    println("最小值是:" + tmpRDD.min())
    println("平均值是:" + tmpRDD.mean())

    //6.TODO: 搜索时间段统计
    println("搜索时间段统计:")
    //6.1 将每条数据切割, 只保留部分小时的数据
    val timeRDD: RDD[(String, Int)] = recordRDD.map(x => {
      val str: String = x.querytime.substring(0, 2)
      str
    })
      //6.2 词频统计, 并做排序
      .map((_, 1))
      .reduceByKey(_ + _)
      .coalesce(1) //全局排序, 需要减少分区为1
      .sortBy(x => x._2, false)
    timeRDD.foreach(println(_))


    //7.关闭上下文对象
    sc.stop()
  }

}
