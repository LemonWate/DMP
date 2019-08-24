package com.Target

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Author HanDong
  *
  * Date 2019/8/24
  *
  * Description
  * json数据归纳格式（考虑获取到数据的成功因素 status=1成功 starts=0 失败）：
  * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
  * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  *
  *
  **/
object T1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val sc = new SparkContext(conf)

    val data: Array[String] = sc.textFile("dir/json.txt").collect
//    val buffer = collection.mutable.ListBuffer[(String,Int)]()
    var list = collection.mutable.ListBuffer[(String,Int)]()


    //循环多个json数据
    for (i <- 0 to data.length-1) {
      val jSONObject: JSONObject = JSON.parseObject(data(i))

      val status: Int = jSONObject.getIntValue("status")
      if (status == 0) return ""
      //解析内部json
      val regeocode = jSONObject.getJSONObject("regeocode")
      if (regeocode == null || regeocode.isEmpty) return ""

      val pois: JSONArray = regeocode.getJSONArray("pois")
      if (pois == null || pois.isEmpty) return null

      //开始处理pois内部数据
      val buffer = collection.mutable.ListBuffer[(String,Int)]()

      for (item <- pois.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append((json.getString("businessarea"),1))
          list:+=((json.getString("businessarea"),1))
        }
      }

      val res1RDD: RDD[(String, Int)] = sc.parallelize(buffer).filter(_._1 != "[]")

      val reduRDD: RDD[(String, Int)] = res1RDD.reduceByKey(_+_)

//      println(reduRDD.collect.toList.toString())

            reduRDD.foreach(println)

    }
    println("********************************")
   val map: Map[String, ListBuffer[(String, Int)]] = list.groupBy(_._1)
    val stringToTuple: Map[String, (String, Int)] = map.mapValues(x => x.reduce((x, y) => {
      (x._1, x._2 + y._2)
    }))
    stringToTuple.map(x=>{
      (x._1,x._2._2)
    }).filter(_._1!="[]")foreach(println)

  }

}
