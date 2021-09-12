package com.test.day04.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/18 17:02
 * @Desc:
 */
object S7FlowerQuery {

  /**
   * 定义样例类
   *
   * @param id
   * @param name
   * @param age
   */
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
    //将RDD转换成DataFrame
    val tupleRDD: RDD[(Int, String, Int)] = inputRDD.map(x => {
      val arr: Array[String] = x.split("\\s+")
      (arr(0).toInt, arr(1), arr(2).toInt)
    })
    //TODO: 隐式转换导入
    import spark.implicits._
    val dataFrame: DataFrame = tupleRDD.toDF("id", "name", "age")
    //测试打印
    println("测试打印")
    dataFrame.printSchema()
    dataFrame.show()

    //TODO:对DataFrame进行SQL风格分析
    //注册临时表
    dataFrame.createOrReplaceTempView("t_person")
    //1.查看name字段的数据, 没有分号
    println("查看name字段的数据, 没有分号")
    val df1: DataFrame = spark.sql("select name from t_person")
    df1.printSchema()
    df1.show()
    //2.查看 name 和age字段数据

    //3.查询所有的name和age，并将age+1

    //4.过滤age大于等于25的

    //5.统计年龄大于30的人数

    //6.按年龄进行分组并统计相同年龄的人数
    println("按年龄进行分组并统计相同年龄的人数")
    spark.sql("select age,count(1) as age_num from t_person group by age").show()

    //TODO:对DataFrame进行DSL风格分析
    //1.查看name字段的数据
    println("DSL:查看name字段的数据")
    dataFrame.select("name").show() //写法1
    dataFrame.select('name).show() //写法2
    //2.查看 name 和age字段数据, 隐式转换可以让String转换成Column对象
    dataFrame.select($"name", $"age").show() //写法3
    dataFrame.select(col("name"), col("age")).show() //写法4, 括号里面写法需要保持一致
    //3.查询所有的name和age，并将age+1
    dataFrame.select($"name", $"age", $"age" + 1).show()
    //4.过滤age大于等于25的
    dataFrame.where("age>=25").show()
    dataFrame.filter("age>=25").show()

    //5.统计年龄大于30的人数
    val cnt: Long = dataFrame.where("age>30").count()
    println("年龄大于30的人数是:" + cnt)

    //6.按年龄进行分组并统计相同年龄的人数
    dataFrame.groupBy("age")
      .count() //自动拼接2个字段
      .show()

    //关闭上下文对象
    sc.stop()
    sc.stop()


  }

}
