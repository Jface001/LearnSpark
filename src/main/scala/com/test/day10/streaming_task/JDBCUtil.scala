package com.test.day10.streaming_task

import java.sql.DriverManager

/**
 * @author jface
 * @create 2021/12/25 20:15
 * @desc JDBC 连接工具类
 *
 */
object JDBCUtil {


  def getMysqlJDBC(condition:String)= {
    Class.forName("com.jdbc.mysql.Driver")
    val conn = DriverManager.getConnection("192.168.1.102", "root", "123456")
    val sql = "select product_id,stock_number,send_number from sz_stock where  product_id in (" + condition + ")"
    val statement = conn.prepareStatement(sql)
    val resultSet = statement.executeQuery()
    val map=scala.collection.mutable.Map[Int,Tuple2[Int,Int]]()
    while(resultSet.next()){
      val product_id = resultSet.getInt(1)
      val stock_number = resultSet.getInt(2)
      val send_number = resultSet.getInt(3)
      map(product_id)=(stock_number,send_number)
    }
    map
  }

  def getImpalaJDBC(condition:String)= {
    //TODO: 根据查询条件 JDBC 查询 Impala 获取 real_number
    Class.forName("com.cloudera.impala.jdbc41.Driver")
    val conn = DriverManager.getConnection("jdbc:impala://xx.xx.xx.xx:21050/rawdata;UseNativeQuery=1", "root", "123456")
    val statement = conn.prepareStatement("select product_id,real_number from sz_stock where  product_id in (" + condition + ")")
    val resultSet = statement.executeQuery()
    val map = scala.collection.mutable.Map[Int, Int]()
    while (resultSet.next()) {
      val product_id = resultSet.getInt(1)
      val real_number = resultSet.getInt(2)
      map(product_id) = real_number
    }
    map
  }


}