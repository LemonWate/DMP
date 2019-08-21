package com.Rept

import com.Utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * @Author HanDong
  * @Date 2019/8/21 
  * @Description
  * 地域分布指标
  **/
object LocationRpt {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //获取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    //对数据进行处理，统计各个指标
    val resRDD: RDD[((String, String), List[Double])] = df.map(row => {
      //取到所有需要的字段
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //key的值是省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")

      val post = RptUtils.request(requestmode, processnode)
      val clik = RptUtils.clik(requestmode, iseffective)
      val ad = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      val res: List[Double] = post ::: clik ::: ad
      ((pro, city), res)
    })
    val res: RDD[((String, String), List[Double])] = resRDD.reduceByKey((x,y)=>(x zip y).map(x=>x._1+x._2))
    // 格式： 省份，城市 原始请求 有效请求 广告请求 参与竞价数 成功竞价数 广告展示量 广告点击量 千人消费 千人成本
    res.collect.foreach(x=>println(x._1._1+","+x._1._2+" "+x._2(0)+" "+x._2(1)+" "+x._2(2)+" "+x._2(5)+" "+x._2(6)+" "+x._2(3)+" "+x._2(4)+" "+x._2(7)+" "+x._2(8)))


  }

}
