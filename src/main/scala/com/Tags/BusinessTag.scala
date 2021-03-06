package com.Tags

import ch.hsr.geohash.GeoHash
import com.Utils.{AmapUtil, JedisConnectionPool, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Author HanDong
  *
  * Date 2019/8/26 
  *
  * Description
  * 商标标签
  **/
object BusinessTag extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
//    val long = Utils2Type.toDouble(row.getAs[String]("long"))
//    val lat = Utils2Type.toDouble(row.getAs[String]("lat"))

    val long =116.310003
    val lat = 39.991957
    if (long >= 73.0 &&
      long <= 135.0 &&
      lat >= 3.0 &&
      lat <= 54.0
    ) {
      // 先去数据库获取商圈
      val business: String = getBusiness(long, lat)
      //判断缓存中是否有此商圈
      if (StringUtils.isNotBlank(business)){
        val lines: Array[String] = business.split(",")
        lines.foreach(x=>list:+=(x,1))
      }
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long: Double, lat: Double): String = {
    // 转换GeoHash字符串
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
    // 去数据库查询
    var business = redis_queryBusiness(geohash)
    // 判断商圈是否为空
    if (business == null || business.length == 0) {
      // 通过经纬度获取商圈
      business = AmapUtil.getBusinessFromAmap(long, lat)
      // 如果调用高德地图解析商圈，那么需要将此次商圈存入redis
      redis_insertBusiness(geohash, business)
    }
    business
  }

  /**
    * 获取商圈信息
    */

  def redis_queryBusiness(geohash: String): String = {
    val jedis = JedisConnectionPool.getConnections()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存储商圈到redis
    */
  def redis_insertBusiness(geoHash: String, business: String): Unit = {
    val jedis = JedisConnectionPool.getConnections()
    jedis.set(geoHash, business)
    jedis.close()
  }

}
