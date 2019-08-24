package com.Target

import com.Utils.JedisConnectionPool
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Author HanDong
  *
  * Date 2019/8/23 
  *
  * Description 
  **/
object test_Redis {
  def main(args: Array[String]): Unit = {
    val jedis = JedisConnectionPool.getConnections()
    val sc = new SparkContext(new SparkConf().setAppName("test_redis").setMaster("local[2]"))
    val sQLContext = new SQLContext(sc)


    val map: Map[String, String] = sc.textFile("D:\\Desktop\\BigData22\\22Spark项目\\项目day01\\Spark用户画像分析\\app_dict.txt").map(_.split("\t", -1)).filter(_.length >= 5).map(arr => (arr(4), arr(1))).collect.toMap


//    println(jedis.set("key1", map.toString()))

    map.foreach(x=>{
      jedis.hset("kk3",x._1,x._2)
    })
    println("ok")




  }

}
