package com.Tags

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Author HanDong
  *
  * Date 2019/8/23 
  *
  * Description 
  **/
object TagsChannel extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]

    //获取渠道ID
    val channelid = row.getAs[Int]("adplatformproviderid")
    channelid match {
      case v  => list:+=("CN"+v,1)
    }
    list
  }
}
