package com.test.day05.sparkSQL

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @Author: Jface
 * @Desc:
 */
object S1WordcountSQL {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()
    //读取文件生成DataSet, 只有一列时 列名自动为 value
    val inputDS: Dataset[String] = spark.read.textFile("src/main/data/words.txt")
    //扁平化单词, 得到新的DataSet
    //TODO: 转换数据类型都需要转换???
    import spark.implicits._
    val wordDS: Dataset[String] = inputDS.flatMap(x => {
      x.split(" ")
    })
    wordDS.printSchema()
    wordDS.show()
    //对新的DataSet进行SQL风格 Wordcount
    println("QL风格 Wordcount")
    println()

    wordDS.createOrReplaceTempView("t_words")
    spark.sql(
      """
        |select value,count(1) cnt
        |from t_words
        |group by value
        |order by cnt desc
        |""".stripMargin).show()

    //对新的DataSet进行DSL风格 Wordcount
    wordDS.groupBy("value")
      .count() //自动拼接一列count名
      .orderBy($"count".desc) //转成column对象
      .show()

    //关闭上下文对象
    spark.stop()

  }

}
