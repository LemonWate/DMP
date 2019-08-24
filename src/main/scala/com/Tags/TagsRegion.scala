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
object TagsRegion extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]

    //获取字段
    val proname: String = row.getAs[String]("provincename")
    if (StringUtils.isNoneBlank(proname)){
      list :+=("ZP"+proname,1)
    }
    val cityname: String = row.getAs[String]("cityname")
    if (StringUtils.isNoneBlank(cityname)){
      list :+=("ZC"+cityname,1)
    }
    list
  }
}
