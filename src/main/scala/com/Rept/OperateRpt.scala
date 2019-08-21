package com.Rept

import com.Utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * @Author HanDong
  * @Date 2019/8/21 
  * @Description
  *             运营指标
  **/
object OperateRpt {
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
    val resRDD: RDD[(String, List[Double])] = df.map(row => {
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

      //key是运营商的name
      val ispname = row.getAs[String]("ispname")

      val post = RptUtils.request(requestmode, processnode)
      val clik = RptUtils.clik(requestmode, iseffective)
      val ad = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      val res: List[Double] = post ::: clik ::: ad
      (ispname, res)
    })
    val res: RDD[(String, List[Double])] = resRDD.reduceByKey((x,y)=>(x zip y).map(x=>x._1+x._1))

    res.collect.foreach(x=>println(x._1+" "+x._2(0)+" "+x._2(1)+" "+x._2(2)+" "+x._2(5)+" "+x._2(6)+" "+x._2(3)+" "+x._2(4)+" "+x._2(7)+" "+x._2(8)))
  }
}
