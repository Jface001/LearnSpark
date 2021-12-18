package com.test.day03.spark_core

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Desc:
 * 以词频统计WordCount程序为例，假设处理的数据如下所示，
 * 包括非单词符号，统计数据词频时过滤非单词的特殊符号并且统计总的个数。
 */
object S6ShareValue {
  def main(args: Array[String]): Unit = {

    //创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //读取文件获取RDD
    val inputRDD: RDD[String] = sc.textFile("src/main/data/input/words2.txt", 2)
    //定义变量保存特殊字符
    val list: List[String] = List(",", ".", "!", "#", "$", "%")
    //TODO: 将上面变量广播到Executor端
    val bc: Broadcast[List[String]] = sc.broadcast(list)
    //定义整数累计器
    val accum: LongAccumulator = sc.longAccumulator("accum")
    //对RDD处理, 过滤空行
    val inputRDD2: RDD[String] = inputRDD.filter(StringUtils.isNotBlank(_))
    //拆分单词
    val flatRDD: RDD[String] = inputRDD2.flatMap(_.split("\\s+"))
    //过滤 ,特殊字符+1,返回只含单词的RDD
    val wordRDD: RDD[String] = flatRDD.filter(x => {
      val list: List[String] = bc.value
      val flag: Boolean = list.contains(x)
      if (flag) { //如果为 true 是特殊字符, 累加器+1,
        accum.add(1)
      }
      !flag
    })

    //对RDD进行词频统计
    val outputRDD: RDD[(String, Int)] = wordRDD.map((_ -> 1)).reduceByKey(_ + _)
    //打印累加器结果和词频统计结果,需要先运行foreach 触发

    outputRDD.foreach(println(_))
    println(accum.value)

    //休眠, 以便于查看 web 页面
    //Thread.sleep(5 * 60 * 1000)
    //关闭上下文对象
    sc.stop()
  }

}
