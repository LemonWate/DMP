package com.other_ETL

/**
  * @Author HanDong
  * @Date 2019/8/20 
  * @Description
  **/
 class Log(sessionid: String,
          advertisersid: Int,
          adorderid: Int,
          adcreativeid: Int,
          adplatformproviderid: Int,
          sdkversion: String,
          adplatformkey: String,
          putinmodeltype: Int,
          requestmode: Int,
          adprice: Double,
          adppprice: Double,
          requestdate: String,
          ip: String,
          appid: String,
          appname: String,
          uuid: String,
          device: String,
          client: Int,
          osversion: String,
          density: String,
          pw: Int,
          ph: Int,
          long: String,
          lat: String,
          provincename: String,
          cityname: String,
          ispid: Int,
          ispname: String,
          networkmannerid: Int,
          networkmannername: String,
          iseffective: Int,
          isbilling: Int,
          adspacetype: Int,
          adspacetypename: String,
          devicetype: Int,
          processnode: Int,
          apptype: Int,
          district: String,
          paymode: Int,
          isbid: Int,
          bidprice: Double,
          winprice: Double,
          iswin: Int,
          cur: String,
          rate: Double,
          cnywinprice: Double,
          imei: String,
          mac: String,
          idfa: String,
          openudid: String,
          androidid: String,
          rtbprovince: String,
          rtbcity: String,
          rtbdistrict: String,
          rtbstreet: String,
          storeurl: String,
          realip: String,
          isqualityapp: Int,
          bidfloor: Double,
          aw: Int,
          ah: Int,
          imeimd5: String,
          macmd5: String,
          idfamd5: String,
          openudidmd5: String,
          androididmd5: String,
          imeisha1: String,
          macsha1: String,
          idfasha1: String,
          openudidsha1: String,
          androididsha1: String,
          uuidunknow: String,
          userid: String,
          iptype: Int,
          initbidprice: Double,
          adpayment: Double,
          agentrate: Double,
          lomarkrate: Double,
          adxrate: Double,
          title: String,
          keywords: String,
          tagid: String,
          callbackdate: String,
          channelid: String,
          mediatype: Int
          ) extends Product with Serializable {
  override def productElement(n: Int): Any = {
    n match {
      case 0 => sessionid
      case 1 => advertisersid
      case 2 => adorderid
      case 3 => adcreativeid
      case 4 => adplatformproviderid
      case 5 => sdkversion
      case 6 => adplatformkey
      case 7 => putinmodeltype
      case 8 => requestmode
      case 9 => adprice
      case 10 => adppprice
      case 11 => requestdate
      case 12 => ip
      case 13 => appid
      case 14 => appname
      case 15 => uuid
      case 16 => device
      case 17 => client
      case 18 => osversion
      case 19 => density
      case 20 => pw
      case 21 => ph
      case 22 => long
      case 23 => lat
      case 24 => provincename
      case 25 => cityname
      case 26 => ispid
      case 27 => ispname
      case 28 => networkmannerid
      case 29 => networkmannername
      case 30 => iseffective
      case 31 => isbilling
      case 32 => adspacetype
      case 33 => adspacetypename
      case 34 => devicetype
      case 35 => processnode
      case 36 => apptype
      case 37 => district
      case 38 => paymode
      case 39 => isbid
      case 40 => bidprice
      case 41 => winprice
      case 42 => iswin
      case 43 => cur
      case 44 => rate
      case 45 => cnywinprice
      case 46 => imei
      case 47 => mac
      case 48 => idfa
      case 49 => openudid
      case 50 => androidid
      case 51 => rtbprovince
      case 52 => rtbcity
      case 53 => rtbdistrict
      case 54 => rtbstreet
      case 55 => storeurl
      case 56 => realip
      case 57 => isqualityapp
      case 58 => bidfloor
      case 59 => aw
      case 60 => ah
      case 61 => imeimd5
      case 62 => macmd5
      case 63 => idfamd5
      case 64 => openudidmd5
      case 65 => androididmd5
      case 66 => imeisha1
      case 67 => macsha1
      case 68 => idfasha1
      case 69 => openudidsha1
      case 70 => androididsha1
      case 71 => uuidunknow
      case 72 => userid
      case 73 => iptype
      case 74 => initbidprice
      case 75 => adpayment
      case 76 => agentrate
      case 77 => lomarkrate
      case 78 => adxrate
      case 79 => title
      case 80 => keywords
      case 81 => tagid
      case 82 => callbackdate
      case 83 => channelid
      case 84 => mediatype
    }

  }

  override def productArity: Int = 85

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Log]

}
