package com.test.day03.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Jface
 * @Date: 2021/8/17 10:10
 * @Desc:
 */
object S2RDDjoin {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.定义2个数据集描述员工和部门
    val empRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhangliu"))
    )
    val deptRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "sales"), (1002, "tech"))
    )
    //3.join测试
    val joinRDD: RDD[(Int, (String, String))] = empRDD.join(deptRDD)
    //Option只有2个结果, Some() 和None
    val leftRDD: RDD[(Int, (String, Option[String]))] = empRDD.leftOuterJoin(deptRDD)

    //4.打印结果
    joinRDD.foreach(println(_))
    println("----我是分隔符----")
    leftRDD.foreach(println(_))

    //5.关闭上下文
    sc.stop()



  }

}
