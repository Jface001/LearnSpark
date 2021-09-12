package com.test.day04.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/18 15:43
 * @Desc:
 */
object S3DataFrameCaseClass {
  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()
    //读取文件形成RDD
    val sc: SparkContext = spark.sparkContext
    val inputRDD: RDD[String] = sc.textFile("src/main/data/input/person.txt")
    //将每行数据转换成样例类对象
    val personRDD: RDD[Person] = inputRDD.map(x => {
      val arr: Array[String] = x.split("\\s+")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })
    //将RDD转换成DataFrame
    //TODO: 需要导入隐式转换
    //personRDD本身是没有 toDF的API,需要导入隐式转换
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF()
    //查看Schema信息
    personDF.printSchema()
    //打印DataFrame数据
    personDF.show()
    //关闭上下文对象
    spark.stop()
    sc.stop()
  }

}
