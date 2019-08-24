package com.Tags

import com.Utils.{JedisConnectionPool, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * Author HanDong
  *
  * Date 2019/8/24 
  *
  * Description 
  **/
object TagsAppRedies extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {


    var list = List[(String, Int)]()

    //解析数据
    val row: Row = args(0).asInstanceOf[Row]
    val jedis: Jedis = args(1).asInstanceOf[Jedis]

    //获取字段数据
    val appid: String = row.getAs[String]("appid")
    val appname: String = row.getAs[String]("appname")

    // 空值判断
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNoneBlank(appid)){
      list:+=("APP"+jedis.get(appid),1)
    }
    list
  }
}
