package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.mllib.linalg.Vector
/**
  * Created by liangyh on 1/10/17.
  */
class RawVector (vectorData: Vector, attachmentData:AnyRef){
  val vector = vectorData
  val attachment = attachmentData
}

object RawVector{
  def apply(vector:Vector, attachment:AnyRef): RawVector = {
    new RawVector(vector, attachment)
  }
}
