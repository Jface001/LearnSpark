package com.test.day03.sougouQ

/**
 * @Author: Jface
 * @Desc: 定义样例类, 用于保存每一条数据
 */

/**
 *  定义一个样例类, 用于保存每一条数据
 * @param querytime 搜索时间
 * @param userId 用户id
 * @param queryWords 搜索关键词
 * @param resultRank 搜索结果搜索排名
 * @param clickRank 用户点击的顺序号
 * @param clickUrl 点击的链接
 */
case class SougouRecord(
                         querytime: String,
                         userId: String,
                         queryWords: String,
                         resultRank: Int,
                         clickRank: Int,
                         clickUrl: String
                       )



