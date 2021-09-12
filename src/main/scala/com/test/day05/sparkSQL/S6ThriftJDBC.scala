package com.test.day05.sparkSQL

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * @Author: Jface
 * @Date: 2021/8/19 20:44
 * @Desc: 用JDBC客户端去连接thriftserver
 */
object S6ThriftJDBC {
  def main(args: Array[String]): Unit = {
    //0.抽取变量
    var conn: Connection = null
    var stat: PreparedStatement = null
    var rs: ResultSet = null
    //1.创建连接对象
    try {
      conn = DriverManager.getConnection("jdbc:hive2://node3:10001")
      //2.执行SQL语句
      stat = conn.prepareStatement("show databases")
      rs = stat.executeQuery
      //3.解析结果并打印
      while (rs.next()) {
        val databaseName: String = rs.getString(1)
        println(databaseName)
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      //关闭程序
      rs.close()
      stat.close()
      conn.close()
    }
  }


}
