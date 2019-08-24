package com.Utils

/**
  * Author HanDong
  *
  * Date 2019/8/23 
  *
  * Description
  *             打标签的统一接口
  **/
trait Tag {
  def makeTags(args:Any*):List[(String,Int)]
}
