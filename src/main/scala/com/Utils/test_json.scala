package com.Utils

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author HanDong
  *
  * Date 2019/8/24 
  *
  * Description 
  **/
object test_json {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val list= List("116.310003,39.991957")
    val rdd = sc.makeRDD(list)
    val bs = rdd.map(t=> {
      val arr = t.split(",")
      AmapUtil.getBusinessFromAmap(arr(0).toDouble,arr(1).toDouble)
    })
    bs.foreach(println)

    val data: RDD[String] = sc.textFile("dir/a.json")
    val value: RDD[String] = data.map(x=>{
      val nObject: JSONObject = JSON.parseObject(x)
      val unit= nObject.getString("status")
      unit
    })
   value.foreach(println)



//    val jSONObject: JSONObject = JSON.parseObject(value(0))
//
//    println(jSONObject.getIntValue("status"))
  }

}
