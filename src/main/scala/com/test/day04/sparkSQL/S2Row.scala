package com.test.day04.sparkSQL

import org.apache.spark.sql.Row

/**
 * @Author: Jface
 * @Date: 2021/8/18 14:57
 * @Desc:
 */
object S2Row {
  def main(args: Array[String]): Unit = {
    //TODO: 创建Row对象
    //方式1
    val row1: Row = Row("张三", 22)
    //方式2
    val row2: Row = Row.fromSeq(Array("李四", 40))

    //TODO: 获取row对象字段
    //方式1
    val name: Any = row1(0)
    val strName: String = name.asInstanceOf[String]
    println(strName)
    val age: Int = row1(1).asInstanceOf[Int]
    println(age)
    //方式2
    val name2: String = row2.getString(0)
    val age2: Int = row2.getInt(1)
    println("name=" + name2, "age=" + age2)
    //方式3  略

  }

}
