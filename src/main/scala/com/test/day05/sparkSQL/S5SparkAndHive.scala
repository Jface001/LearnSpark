package com.test.day05.sparkSQL

import org.apache.spark.sql.SparkSession

/**
 * @Author: Jface
 * @Date: 2021/8/19 20:36
 * @Desc:
 */
object S5SparkAndHive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://node3:9083")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    spark.sql("show databases").show()

    spark.stop()
  }


}
