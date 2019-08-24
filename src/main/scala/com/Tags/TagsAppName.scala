package com.Tags

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * Author HanDong
  *
  * Date 2019/8/23 
  *
  * Description 
  **/
object TagsAppName extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    //解析数据
    val row: Row = args(0).asInstanceOf[Row]
//    val appmap= args(1).asInstanceOf[Broadcast[Map[String, String]]]  //  此处使用redis
    val conn: Jedis = args(1).asInstanceOf[Jedis]
    //获取字段数据
    val appid: String = row.getAs[String]("appid")
    val appname: String = row.getAs[String]("appname")



    // 空值判断
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNoneBlank(appid)){
      val appnm: String = conn.hget("App",appid)
      println(appnm)
      list:+=("APP"+appnm,1)
//      list:+=("APP"+appmap.value.getOrElse(appid,appid),1)
    }
    conn.close()
    list
  }
}
