package com.jdbc


import com.Utils.MysqlPoolUtils

/**
  * Author HanDong
  *
  * Date 2019/8/22 
  *
  * Description 
  **/
object Test_druid {
  def main(args: Array[String]): Unit = {
     val conn = MysqlPoolUtils.getConnection.get
    val statement = conn.prepareStatement("insert into test values(\"ynn\",18)")

    statement.execute()

    conn.close()
  }

}
