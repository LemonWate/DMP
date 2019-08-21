package com.Utils

/**
  * @Author HanDong
  * @Date 2019/8/21
  * @Description
  * 指标方法
  **/
object RptUtils {
  //此方法处理请求数
  def request(requestmode: Int, processnode: Int): List[Double] = {
    if (requestmode == 1) {
      processnode match {
        case 1 =>return List(1, 0, 0)
        case 2 =>return List(1, 1, 0)
        case 3 =>return List(1, 1, 1)
        case _ =>return List(0, 0, 0)
      }
    }
    List(0, 0, 0)
  }

  //此方法处理展示点击数
  def clik(requestmode: Int, iseffective: Int): List[Double] = {
    if (iseffective == 1) {
      requestmode match {
        case 2 =>return List(1, 0)
        case 3 =>return List(0, 1)
        case _ =>return List(0, 0)
      }
    }
    List(0, 0)
  }

  //此方法处理竞价操作
  def Ad(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
         adorderid: Int, winPrice: Double, adpayment: Double): List[Double] = {
    if (iseffective == 1 && isbilling == 1) {
      isbid match {
        case 1 =>return List(1, 0, 0, 0)
        case _ => iswin match {
          case 1 => adorderid match {
            case 0 =>return List(0, 0, winPrice / 1000, adpayment / 1000)
            case _ =>return List(0, 1, winPrice / 1000, adpayment / 1000)
          }
          case _ =>return List(0, 0, 0, 0)
        }
      }
    } else {
      List(0, 0, 0, 0)
    }
  }




}

