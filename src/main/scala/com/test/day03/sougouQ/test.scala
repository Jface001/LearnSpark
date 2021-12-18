package com.test.day03.sougouQ

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable


/**
 * @Author: Jface
 * @Desc:
 */
object test {
  def main(args: Array[String]): Unit = {
    //测试分词效果
    //放入分词
    val terms: util.List[Term] = HanLP.segment("我想过上过儿过的生活")
    //转化成 Scala 的集合
    val term: mutable.Buffer[Term] = terms.asScala
    //获取term中的单词
    term.foreach(x => println(x.word))
    //测试一下是否切割成功
    val a = "[我的天哪]"
    println(a.stripSuffix("]").stripPrefix("["))

  }

}
