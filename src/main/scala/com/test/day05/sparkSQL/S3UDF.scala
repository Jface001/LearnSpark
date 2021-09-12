package com.test.day05.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/19 15:46
 * @Desc:
 */
object S3UDF {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "8") //设置Shuffle的并行度为8, 否则默认为200
      .master("local[*]")
      .getOrCreate()
    //2.加载文件生产DataSet
    val inputDS: Dataset[String] = spark.read.textFile("src/main/data/input/udf.txt")
    //TODO: 3.用SQL风格实现UDF函数, 小写单词-->大写单词
    spark.udf.register("smallToBig",
      (str: String) => {
        str.toUpperCase
      }
    )

    inputDS.createOrReplaceTempView("t_udf")
    spark.sql(
      """select
        |value,
        |smallToBig(value) as big_value
        |from t_udf
        |""".stripMargin).show()

    //TODO: 4.用DSL风格实现UDF函数,小写单词-->大写单词
    println()
    //导类
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val smallToBig2: UserDefinedFunction = udf((str: String) => str.toUpperCase)

    inputDS.select(
      $"value",
      smallToBig2($"value").as("big_value2")
    ).show()

    //关闭上下问对象
    spark.stop()

  }

}
