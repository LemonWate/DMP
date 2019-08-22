package com.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

/**
  * Author HanDong
  * Date 2019/8/21
  * Description
  *             spark foreachPartition 把df 数据插入到mysql
  *
  **/
object forearchPartitionTest {
  case class TopSongAuthor(songAuthor:String, songCount:Long)


  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/atest?user=root&password=123456")
  }

  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def insertTopSong(list:ListBuffer[TopSongAuthor]):Unit ={

    var connect:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connect = getConnection()
      connect.setAutoCommit(false)
      val sql = "insert into topSinger(song_author, song_count) values(?,?)"
      pstmt = connect.prepareStatement(sql)
//      for(ele  e.printStackTrace()
    }finally {
      release(connect, pstmt)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val gedanDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306").option("dbtable", "baidusong.gedan").option("user", "root").option("password", "123456").option("driver", "com.mysql.jdbc.Driver").load()
    //    mysqlDF.show()
    val detailDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306").option("dbtable", "baidusong.gedan_detail").option("user", "root").option("password", "123456").option("driver", "com.mysql.jdbc.Driver").load()

    val joinDF = gedanDF.join(detailDF, gedanDF.col("id") === detailDF.col("gedan_id"))

    //    joinDF.show()
    import spark.implicits._
    val resultDF = joinDF.groupBy("song_author").agg(count("song_name").as("song_count")).orderBy($"song_count".desc).limit(100)
    //    resultDF.show()


    resultDF.foreachPartition(partitionOfRecords =>{
      val list = new ListBuffer[TopSongAuthor]
      partitionOfRecords.foreach(info =>{
        val song_author = info.getAs[String]("song_author")
        val song_count = info.getAs[Long]("song_count")

        list.append(TopSongAuthor(song_author, song_count))
      })
      insertTopSong(list)

    })

    sc.stop()
  }
}
