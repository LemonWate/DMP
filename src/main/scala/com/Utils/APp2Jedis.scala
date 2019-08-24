package com.Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Author HanDong
  *
  * Date 2019/8/24 
  *
  * Description 
  **/
object APp2Jedis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //
    val dict: RDD[String] = sc.textFile("D:\\Desktop\\BigData22\\22Spark项目\\项目day01\\Spark用户画像分析\\app_dict.txt")

    val map = dict.map(_.split("\t", -1))
      .filter(_.length >= 5).foreachPartition(arr => {
      val jedis: Jedis = JedisConnectionPool.getConnections()
      arr.foreach(x => {
        jedis.set(x(4), x(1))
      })
      jedis.close()
    }
    )
    sc.stop()

  }

}
