package com.test.day10.streaming_task

import com.google.gson.Gson


/**
 * @author jface
 * @create 2021/12/25 19:09
 * @desc
 *
 */
object HandleMessage {

  def handleMessage2CaseClass(jsonStr: String)={
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[JsonMessge])
  }

}
