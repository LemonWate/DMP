package com.Utils

/**
  * @Author HanDong
  * @Date 2019/8/20 
  * @Description
  *             数据类型转化
  **/
object Utils2Type {
  //String转化Int
  def toInt(str:String): Int ={
    try {
      str.toInt
    }catch {
      case _:Exception=> 0
    }
  }


  //String转换Double
  def toDouble(str:String): Double ={
    try {
      str.toDouble
    }catch {
      case _:Exception=> 0.0
    }
  }

}
