package com.test.day05.sparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/19 10:23
 * @Desc: 读取电影评分数据案例
 */
object S2Movie {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "8") //设置Shuffle的并行度为8, 否则默认为200
      .master("local[*]")
      .getOrCreate()
    //读取电影评分数据
    val inputDS: Dataset[String] = spark.read.textFile("src/main/data/input/rating_100k2.data")
    //将数据转换成具有Schema信息的数据, 封装到DataFrame中
    import spark.implicits._ //DS和DF需要调用函数, 可能需要隐式转换参数
    val df: DataFrame = inputDS.map(x => {
      val arr: Array[String] = x.split("::")
      (arr(0), arr(1), arr(2).toInt, arr(3).toLong)
    }).toDF("user_id", "movie_id", "score", "time")

    //用基于SQL风格方式分析, 获取top10电影
    df.createOrReplaceTempView("t_movie")
    //优化方式2, 只对sparkSQL模块起作用.
    spark.sql("set spark.sql.shuffle.partitions=8")
    spark.sql(
      """
        |select
        |movie_id,
        |round(avg(score),2) as avg_score,
        |count(user_id) as cnt
        |from t_movie
        |group by movie_id
        |having cnt>200
        |order by avg_score desc
        |""".stripMargin).show()

    //用基于DSL风格方式分析,获取top10电影
    //基于上述SQL的执行计划分析得出顺序, 需要导包
    import org.apache.spark.sql.functions._
    df.groupBy("movie_id")
      .agg(
        count("user_id").as("cnt"), //count
        round(avg("score"), 2).as("avg_score") //avg
      )
      .where("cnt>200")
      .orderBy($"avg_score".desc)
      .show()

    Thread.sleep(5 * 60 * 1000)

    //关闭上下文对象
    spark.stop()
  }

}
