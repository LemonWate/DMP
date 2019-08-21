package com.Rept

import com.Utils.RptUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * @Author HanDong
  * @Date 2019/8/21 
  * @Description
  *             媒体分析
  **/
object MediaRpt {
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
    //获取字典数据
    val lines: RDD[String] = sc.textFile("D://Desktop/BigData22/22Spark项目/项目day01/Spark用户画像分析/app_dict.txt")

    val idAndNameRDD: RDD[(String, String)] = lines.filter(_.length > 4).map(x => {
      val sts: Array[String] = x.split("\t")
      var name: String = sts(1).trim
      var id: String = sts(4).trim

      (id, name)
    })
    val dic: Map[String, String] = idAndNameRDD.collect.toMap

    //广播字典数据
    val bro_dic: Broadcast[Map[String, String]] = sc.broadcast(dic)


    //获取指标数据
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
      //整合指标数据
      val post = RptUtils.request(requestmode, processnode)
      val clik = RptUtils.clik(requestmode, iseffective)
      val ad = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      val res: List[Double] = post ::: clik ::: ad

      //key的值由appname决定，当appname不存在时，要根据appid去字典查找其name
      val appName = row.getAs[String]("appname")
      val appId = row.getAs[String]("appid")

      def transform(name: String, id: String): String = {
        if (name != "") {
          return name
        } else {
          return bro_dic.value.getOrElse(id, "其他")
        }
      }

      (transform(appName, appId), res)
    })
    //对数据进行聚合
    val res: RDD[(String, List[Double])] = resRDD.reduceByKey((x, y) => (x zip (y)).map(t => t._1 + t._2))

    res.collect.foreach(x => println(x._1 + " " + x._2(0) + " " + x._2(1) + " " + x._2(2) + " " + x._2(5) + " " + x._2(6) + " " + x._2(3) + " " + x._2(4) + " " + x._2(7) + " " + x._2(8)))


    sc.stop()
  }


}
