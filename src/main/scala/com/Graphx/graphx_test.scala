package com.Graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author HanDong
  *
  * Date 2019/8/26 
  *
  * Description
  *           图计算案例
  **/
object graphx_test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val sc = new SparkContext(conf)

    //构造点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("科比", 36)),
      (2L, ("霍华德", 34)),
      (9L, ("詹姆斯", 35)),
      (6L, ("库里", 31)),
      (133L, ("哈登", 30)),
      (138L, ("内马尔", 36)),
      (16L, ("法尔考", 35)),
      (44L, ("C·luonaerd", 30)),
      (5L, ("高斯林", 60)),
      (7L, ("奥的司机", 55)),
      (158L, ("王浩", 34))
    ))

    //构造边的集合
    val edag: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

      //构造图
    val grap = Graph(vertexRDD,edag)

    //取每个边上最大的顶点
    val vertices: VertexRDD[VertexId] = grap.connectedComponents().vertices
    vertices.join(vertexRDD).map{
      case (userId,(conId,(name,age))) =>{
        (conId,List(name,age))
      }
    }.reduceByKey(_:::_).foreach(println)
  }

}
