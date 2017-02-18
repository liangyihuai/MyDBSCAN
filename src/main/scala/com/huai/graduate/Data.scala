package com.huai.graduate

import scala.beans.BeanProperty

/**
  * Created by liangyh on 2/16/17.
  */
class Data (aPhoneNum:String, aBaseID:String,
            aTime:Long, aLongitude:Double, aLatitude:Double){

  @BeanProperty
  val phoneNum:String =aPhoneNum
  @BeanProperty
  val baseID:String = aBaseID
  @BeanProperty
  val time:Long = aTime
  @BeanProperty
  val longitude:Double = aLongitude
  @BeanProperty
  val latitude:Double = aLatitude

}

object Data{
  def apply(phoneNum:String,
            baseID:String, time:Long,
            longitude:Double, latitude:Double
           ): Data = new Data(phoneNum, baseID, time, longitude, latitude)
}
