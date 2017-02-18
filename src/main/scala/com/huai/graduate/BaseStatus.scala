package com.huai.graduate

/**
  * Created by liangyh on 2/16/17.
  */
class BaseStatus(aBaseID:String,aStartTime:Long,aEndTime:Long) {

  /**
    * 基站ID
    */
  val baseID:String = aBaseID

  /**
    * 接入起始时间
    */
  val startTime:Long = aStartTime

  /**
    * 接入结束时间
    */
  var endTime:Long = aEndTime

  var geoHashStr:String = _;

  def this(data:Data) = this(data.baseID, data.time, 0)
}
