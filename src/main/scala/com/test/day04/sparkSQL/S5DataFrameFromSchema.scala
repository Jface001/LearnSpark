package com.test.day04.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author: Jface
 * @Desc:
 */
object S5DataFrameFromSchema {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()
    //读取文件形成RDD
    val sc: SparkContext = spark.sparkContext
    val inputRDD: RDD[String] = sc.textFile("src/main/data/input/person.txt")
    //将每行转换成一个row对象
    val rowRDD: RDD[Row] = inputRDD.map((x) => {
      val arr: Array[String] = x.split("\\s+")
      arr
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })
    //定义structType 和structField, 组成Schema
    val schema: StructType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    //将RDD[row]转换成DataFrame
    val dataFrame: DataFrame = spark.createDataFrame(rowRDD, schema)
    //查看Schema信息
    dataFrame.printSchema()
    //打印DataFrame
    dataFrame.show()

    //关闭上下文对象
    sc.stop()
    spark.stop()
  }

}
