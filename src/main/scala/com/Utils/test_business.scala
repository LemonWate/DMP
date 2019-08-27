package com.Utils

import com.Tags.BusinessTag
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author HanDong
  *
  * Date 2019/8/26 
  *
  * Description 
  **/
object test_business {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)
    val df: DataFrame = sQLContext.read.parquet("D:\\aa")

    df.map(row=>{
      val business = BusinessTag.makeTags(row)
      business
    }).foreach(println)
  }

}
