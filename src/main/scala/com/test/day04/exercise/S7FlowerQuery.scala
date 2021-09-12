package com.test.day04.exercise

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/18 17:44
 * @Desc: 案例1:花式查询, 学习SparkSQL 中的SQL风格数据分析和DSL风格分析
 */
object S7FlowerQuery {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    //2.读取数据获取RDD
    val inputRDD: RDD[String] = sc.textFile("src/main/data/input/person.txt")

    //3.将RDD转换成DataFrame
    //3种转换方式: 样例类对象反射, 元组 row + schema, 使用第3种练习
    val row: RDD[Row] = inputRDD.map(x => {
      val arr: Array[String] = x.split("\\s+")
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })

    val schema: StructType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val dataFrame: DataFrame = spark.createDataFrame(row, schema)
    //打印测试
    println("打印测试")
    dataFrame.show()

    //TODO: 4.对RDD进行SQL风格数据分析
    println("对RDD进行SQL风格数据分析")
    //前提: 创建临时视图
    dataFrame.createOrReplaceTempView("t_person")
    //1.查看name字段的数据
    spark.sql("select name from t_person").show()
    //2.查看 name 和age字段数据
    spark.sql("select name,age from t_person").show()
    //3.查询所有的name和age，并将age+1
    spark.sql("select name,age,age+1 as new_age  from t_person ").show()
    //4.过滤age大于等于25的
    spark.sql("select age from t_person where age>=25").show()
    //5.统计年龄大于30的人数
    spark.sql("select count(1) from t_person where age > 30 ").show()
    //6.按年龄进行分组并统计相同年龄的人数
    spark.sql("select age, count(1) from t_person group by age").show()

    //TODO: 5.对RDD进行DSL风格数据分析
    println("对RDD进行DSL风格数据分析")
    //1.查看name字段的数据
    dataFrame.select("name").show() //写法1
    import spark.implicits._ // 写法2和写法3依赖的隐式转换
    dataFrame.select($"name").show() //写法2
    dataFrame.select('name).show() //写法3
    dataFrame.select(col("name")).show() //写法4
    //2.查看 name 和age字段数据
    dataFrame.select("name", "age").show()
    //3.查询所有的name和age，并将age+1
    dataFrame.select('name, 'age, 'age + 1)
    //4.过滤age大于等于25的人数
   dataFrame.where("age>=25").show()
    //5.统计年龄大于30的人数
    println(dataFrame.filter("age>30").count())
    //6.按年龄进行分组并统计相同年龄的人数
    dataFrame.groupBy("age")
      .count() //自动拼接显示2个字段
      .show()

    //6.关闭上下文对象
    sc.stop()
    spark.stop()
  }

}
