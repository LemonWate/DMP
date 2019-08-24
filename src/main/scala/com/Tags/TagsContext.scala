package com.Tags

import com.Utils.{JedisConnectionPool, TagsUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author HanDong
  *
  * Date 2019/8/23 
  *
  * Description:   打标签
  *
  **/
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath, outputPath, dirPath, stopPath) = args

    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)

    //读取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    val map = sc.textFile(dirPath).map(_.split("\t", -1)).filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    val jedis = JedisConnectionPool.getConnections()
    map.foreach(x => {
      val appid = x._1
      val name = x._2
      jedis.hset("AppInfo233", appid, name)
    })
    jedis.close()

    //    //广播
    //    val broadcast = sc.broadcast(map)
    //获取停用词库
    val stopword = sc.textFile(stopPath).map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //过滤符合id的数据
    df.filter(TagsUtils.OneUserId)
      .mapPartitions(part => {
        val con = JedisConnectionPool.getConnections()
        val tuples= part.map(row => {
          //取出用户id
          val userId = TagsUtils.getOneUserId(row)
          //接下来通过row数据  打上所有标签（按需求）
          val adList = TagsAd.makeTags(row)

          //      val appName = TagsAppName.makeTags(row, broadcast)
          val appName = TagsAppName.makeTags(row, con)

          val channel = TagsChannel.makeTags(row)

          val facility = TagsFacility.makeTags(row)

          val keyWords = TagsKeyWords.makeTags(row, bcstopword)

          val region = TagsRegion.makeTags(row)
          (userId, adList ::: appName ::: channel ::: facility ::: keyWords ::: region)
        })
        con.close()
        tuples

      })
    sc.stop()

  }

}
