package com.test.day04.exercise

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: Jface
 * @Desc: 通过样例类创建DataFrame对象
 */
object S3DataFrameCaseClass {

  /**
   * 定义样例类, 用于存储文本文件中的每一条数据
   *
   * @param id
   * @param name
   * @param age
   */
  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //1.创建上下问对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    //2.读取文本文件, 获取RDD
    val inputRDD: RDD[String] = sc.textFile("src/main/data/input/person.txt")
    println("RDD打印")
    inputRDD.foreach(println(_))
    //3.将RDD转换成PersonRDD
    val personRDD: RDD[Person] = inputRDD.map(x => {
      val arr: Array[String] = x.split("\\s+")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })
    //4.将RDD转换成DataFrame
    //TODO: 需要隐式类型转换
    import spark.implicits._
    val dataFrame: DataFrame = personRDD.toDF()
    //5.打印测试
    println("DataFrame打印")
    dataFrame.printSchema()
    dataFrame.show()
    //6.关闭上下文对象
    sc.stop()
    spark.stop()
  }

}
