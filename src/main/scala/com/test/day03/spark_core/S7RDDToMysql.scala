package com.test.day03.spark_core

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * @Author: Jface
 * @Desc:
 */
object S7RDDToMysql {
  def main(args: Array[String]): Unit = {
    //创建上下文对象和数据库连接对象,预编译对象
    var conn: Connection = null
    var stat: PreparedStatement = null

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //创建表
    //准备数据包
    val tuples = List("libai" -> 16, "dufu" -> 20)
    //将集合转换成RDD
    val inputRDD: RDD[(String, Int)] = sc.parallelize(tuples)
    //保存数据到MySQL表
    inputRDD.foreachPartition(par => {
      try {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/day0816", "root",
          "root")
        stat = conn.prepareStatement("insert into t_student value(?,?)")
        par.foreach(stu => {
          stat.setString(1, stu._1)
          stat.setInt(2, stu._2)
          stat.addBatch()
        })
        stat.executeBatch()
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
      finally {
        conn.close()
        stat.close()
      }
    })

    //TODO:从MySQL读取数据
    //定义一个函数, 用于获取conn连接

    val getConnection = () => {
      DriverManager.getConnection("jdbc:mysql://localhost:3306/day0816", "root",
        "root")
    }
    //定义一个函数, 用于解析SQL结果
    val mapRow = (rs: ResultSet) => {
      val name: String = rs.getString(1)
      val age: Int = rs.getInt(2)
      (name, age)
    }

    val resultRDD = new JdbcRDD[(String, Int)](
      sc,
      getConnection,
      "select * from t_student where age>=? and age<= ? ;",
      16,
      19,
      2,
      mapRow
    )
    //打印RDD的数据
    resultRDD.foreach(println(_))

    //关闭上下文对象
    sc.stop()
  }

}
