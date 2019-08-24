package com.Tags

import com.Utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * Author HanDong
  *
  * Date 2019/8/23 
  *
  * Description 
  **/
object TagsKeyWords extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    val keywords: Array[String] = row.getAs[String]("keywords").split("\\|")

    keywords.filter(word=>{
      word.length>=3 && word.length<=8 && !stopword.value.contains(word)
    })
      .foreach(word=> list:+=("K"+word,1))
    list
  }
}
