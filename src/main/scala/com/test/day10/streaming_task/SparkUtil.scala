package com.test.day10.streaming_task
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jface
 * @create 2021/12/19 18:30
 * @desc
 *
 */
object SparkUtil {

def getSpark(conf: SparkConf)={
 val sc = new SparkContext(conf)
 val ssc = new StreamingContext(sc, Seconds(5))
 ssc
}
}
