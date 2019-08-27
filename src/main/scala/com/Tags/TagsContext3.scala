package com.Tags

import com.Utils.TagsUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author HanDong
  *
  * Date 2019/8/27 
  *
  * Description 
  **/
object TagsContext3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("目录不匹配，退出")
      sys.exit()
    }
    val Array(inputPath, outputPath, dirPath, stopPath, days) = args

    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)

    //加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName: String = load.getString("hbase.TableName")

    //创建hadoop任务

    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.host"))

    //创建HBASEConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin

    //判断HBASE表是否可用
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
      //创建表
      val tagsDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")

      tagsDescriptor.addFamily(descriptor)
      hbadmin.createTable(tagsDescriptor)
      hbadmin.close()
      hbconn.close()
    }

    //创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    // 读取字段文件
    val map = sc.textFile(dirPath).map(_.split("\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    // 将处理好的数据广播
    val broadcast = sc.broadcast(map)

    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 过滤符合Id的数据
    val baseRDD = df.filter(TagsUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .map(row => {
      val userList: List[String] = TagsUtils.getAllUserId(row)
      (userList,row)
    })

    //构建点的集合
    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(x => {
      val row = x._2
      //所有标签
      val adList = TagsAd.makeTags(row)
//      val appList = TagsAppName.makeTags(row, broadcast)
      val keywordList = TagsKeyWords.makeTags(row, bcstopword)
      val dvList = TagsFacility.makeTags(row)
      val loactionList = TagsRegion.makeTags(row)
      val business = BusinessTag.makeTags(row)
      val AllTag = adList ++ keywordList ++ dvList ++ loactionList ++ business
      // // 保证其中一个点携带者所有标签，同时也保留所有userId
      val VD: List[(String, Int)] = x._1.map((_, 0)) ::: AllTag
      // // 处理所有的点集合
      x._1.map(uId => {
        // 保证一个点携带标签 (uid,vd),(uid,list()),(uid,list())
        if (x._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
    //构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(x => {
      x._1.map(uId => Edge(x._1.head.hashCode.toLong, uId.hashCode.toLong, 0))
    })
//    edges.take(20).foreach(println)

    //构建图
    val graph = Graph(vertiesRDD,edges)

    //取出顶点 使用的是图计算中的连通图算法
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //处理所有的标签id
    vertices.join(vertiesRDD).map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      //聚合标签
      (list1:::list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(20).foreach(println)



    sc.stop()



  }

}
