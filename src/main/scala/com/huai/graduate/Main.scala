package com.huai.graduate

import ch.hsr.geohash.GeoHash
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by liangyh on 2/16/17.
  */
class Main {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("GraduateProject")
    val sc = new SparkContext(conf)
    val data = sc.textFile("")
  }

  def start(data:RDD[String]):Unit ={
    val validInfo:RDD[Data] = getValidInfo(data, Main.phoneIndex, Main.baseIDIndex,
        Main.timeIndex, Main.longitudeIndex, Main.latitudeIndex)

    validInfo
      .filter(data =>{
        data.time > Main.BEGIN_TIMESTAMP && data.time < Main.END_TIMESTAMP
      })
      .map(data => {(
        data.phoneNum, data)
      })
      .combineByKey(
        value=>List[Data](),
        (list:List[Data], data:Data)=> data::list,
        (list1:List[Data],list2:List[Data]) => list1:::list2
      )
      /*sort by time of the value*/
      .map(item =>{
        (item._1, item._2.sortWith((data1, data2)=>{data1.time < data2.time}))
      })

  }

  def getValidInfo(data:RDD[String],
                       phoneIndex:Int, baseIDIndex:Int,
                       timeIndex:Int, longitudeIndex:Int,
                       latitudeIndex:Int):RDD[Data] ={
    data.filter(line =>{!line.isEmpty && line.charAt(line.length-1) != 'N'}).map(line => {
      val fields = line.split("|")
      val phoneNum = fields(phoneIndex)
      val baseID = fields(baseIDIndex)
      val time = fields(timeIndex).toLong
      val longitude = fields(longitudeIndex).toDouble
      val latitude = fields(latitudeIndex).toDouble
      Data(phoneNum, baseID, time, longitude, latitude)
    })
  }

  /**
    * 构建某一个用户的网格状态
    *
    * 需要注意的是，该函数针对的是网格的状态而不是一条数据的状态。
    * 用户的状态变化不应该是某一个网格的状态的改变，而是在多个网格之间。
    * @param data (string,list)-->(phoneNum, list)
    *
    */
  def generateStatus(data:(String,List[Data])): ArrayBuffer[BaseStatus] ={
    //存放结束时间已经确定的状态
    val stateList = ArrayBuffer.empty[BaseStatus]
    val firstData = data._2(0)
    //找出t0时刻连接基站所在网格;
    var gridA = GeoHash.withBitPrecision(firstData.latitude, firstData.longitude, SIGNIFICANT_BITS)
    //获取邻居网格
    val adjacentKeysOfGridA = gridA.getAdjacent().map(getKeyFromGeohashStr(_))
    //对a及其邻居构建状态网格状态,并将这些状态加入到 gridStatusMap 中;
    var gridStatusMap = buildBaseStatus(gridA, adjacentKeysOfGridA, firstData)

    for(i <- 1 to data._2.size){
      val dataB = data._2(i)
      val gridB = GeoHash.withBitPrecision(dataB.latitude, dataB.longitude, SIGNIFICANT_BITS)
      val adjacentKeysOfGridB = gridB.getAdjacent().map(getKeyFromGeohashStr(_))

      if(gridB.equals(gridA)){//a,b 对应相同网格
        //更新 gridStatusMap 中 a 及其邻居的状态,将结束时间设为 ti ;
        gridStatusMap.values.foreach(_.endTime = dataB.time)
      }else if(haveCommonAdjacent(adjacentKeysOfGridA, adjacentKeysOfGridB)){//a,b有共同的邻居
        //更新 gridStatusMap 中 a,b 共同邻居所对应状态的结束时间;
        val commonNeighbours = commonAdjacents(adjacentKeysOfGridA, adjacentKeysOfGridB)
        commonNeighbours.foreach(gridStatusMap.get(_).get.endTime = dataB.time)

        if(!isNeighbour(adjacentKeysOfGridA, gridB)){//此时 a,b 互不为邻居
          //将a及只属于a的邻居所对应的状态从gridStatusMap中移出并加入到stateList中
          moveDataFromCacheToList(gridStatusMap, stateList, (gridKey)=> (!commonNeighbours.contains(gridKey)))

          //为b及只属于 b 的邻居构建状态 , 并加入 gridStatusMap 中 ;
          val adjacentGridKeysNotIntGridA = for(gridKey <- adjacentKeysOfGridB
                                           if(!commonNeighbours.contains(gridKey))) yield gridKey
          gridStatusMap = buildBaseStatus(gridB, adjacentGridKeysNotIntGridA, dataB)

        }else{//a，b互为邻居
          //将仅属于 a 的邻居状态从 gridStatusMap 移到 stateList 中
          for(key <- adjacentKeysOfGridA if(!commonNeighbours.contains(key))){
            stateList += gridStatusMap.get(key).get
          }
          //为b邻居中仅属于b的网格生成状态,并加入gridStatusMap中;
          val adjacentGridKeysNotIntGridA = for(gridKey <- adjacentKeysOfGridB
                                                if(!commonNeighbours.contains(gridKey))) yield gridKey
          gridStatusMap = buildBaseStatus(gridB, adjacentGridKeysNotIntGridA, dataB, false)
        }
      }else{//此时 a,b 无共同邻居
        //将 a 及其邻居的状态从 gridStatusMap 移到 stateList中
        moveDataFromCacheToList(gridStatusMap, stateList)
        //为 b 及其邻居网格生成状态 , 并加入 cacheList 中 .
        gridStatusMap = buildBaseStatus(gridB, adjacentKeysOfGridB, dataB)
      }
      // 用 b 更新上一时刻所在网格
      gridA = gridB
    }
    moveDataFromCacheToList(gridStatusMap, stateList)

    stateList
  }

  /**
    * 将某网格和它的邻居网格所缓存的状态转移到list中
    * @param cacheMap
    * @param list
    * @param filter 过滤条件
    */
  private[this] def moveDataFromCacheToList(cacheMap:mutable.HashMap[String, BaseStatus],
                                          list:ArrayBuffer[BaseStatus],
                                            filter:((String)=> Boolean)= ((key:String)=>true)):Unit={
    cacheMap.keysIterator.filter(filter).foreach(list += cacheMap.get(_).get)
  }

  /**
    * 为某个网格以及它的邻居网格构建网格状态
    * @param gridA
    * @param adjacentGridsKeys
    * @param data
    * @return
    */
  private[this] def buildBaseStatus(gridA:GeoHash,
                                    adjacentGridsKeys:Array[String],
                                    data:Data,
                                    isBuildForItself:Boolean = true): mutable.HashMap[String, BaseStatus] ={

    val gridStatusMap = new mutable.HashMap[String, BaseStatus]()
    if(isBuildForItself){
      //构建a网格的网络状态
      val gridABaseStatus = new BaseStatus(data)
      gridStatusMap += (getKeyFromGeohashStr(gridA) -> gridABaseStatus)
    }

    //构建邻居网格的网络状态
    adjacentGridsKeys.foreach(adjacentKey =>{
      val neighbourBaseStatus = new BaseStatus(data)
      gridStatusMap += (adjacentKey -> neighbourBaseStatus)
    })
    gridStatusMap
  }

  private[this] def haveCommonAdjacent(adjacentKeysOfGridA:Array[String],
                                       adjacentKeysOfGridB:Array[String]):Boolean ={
    for(adjOfA <- adjacentKeysOfGridA){
      for(adjOfB <- adjacentKeysOfGridB){
        if(adjOfA == adjOfB){
          return true
        }
      }
    }
    false
  }

  /**
    * to look up the neighbour grids that gridA and gridB both have.
    *
    * @param adjacentKeysOfGridA
    * @param adjacentKeysOfGridB
    * @return
    */
  private[this] def commonAdjacents(adjacentKeysOfGridA:Array[String],
                                    adjacentKeysOfGridB:Array[String]): mutable.HashSet[String] ={

    var result = new mutable.HashSet[String]()
    for(adjOfGridA <- adjacentKeysOfGridA ){
      for(adjOfGridB <- adjacentKeysOfGridB){
        if(adjOfGridA.equals(adjOfGridB)){
          result += adjOfGridA
        }
      }
    }
    result
  }

  private[this] def isNeighbour(adjacentKeysOfGridA:Array[String], gridB:GeoHash): Boolean ={
    val keyOfB = gridB.toBinaryString
    adjacentKeysOfGridA.foreach(keyOfA => {
      if(keyOfA == keyOfB)return true
    })
    false
  }


  private[this] def getKeyFromGeohashStr(geoHash: GeoHash):String ={
    if(geoHash == null)throw new NullPointerException()
    geoHash.toBinaryString()
  }

  def statusFilter(statusList:ArrayBuffer[BaseStatus]):ArrayBuffer[BaseStatus] ={
    val groupedData:Map[String, ArrayBuffer[BaseStatus]] = statusList.groupBy[String](_.baseID)
    groupedData.foreach(item => {
      val tempArray = item._2
      //若同一组里的相邻两状态间隔小于某一阈值 (30 分钟 )
      //则将前一状态的结束时间设为后一状态的结束时间,并删除原来的后一种状态
      for(i <- 0 until tempArray.size){
        if(tempArray(i).endTime - tempArray(i+1).endTime >= 1800000){
          tempArray(i).endTime = tempArray(i+1).endTime
          tempArray(i+1) = null
        }
      }
    })
  }



  private val SIGNIFICANT_BITS = 31
  /**
    * one phone number's relationship between a base and the other all bases.
    * Note: it's one phone number's
    */
  def buildBaseNeighbourRelationShip(phoneNumAndData:(String, List[Data])): mutable.HashMap[String, mutable.Set[Data]] ={
    val neighbourRelationShip = new mutable.HashMap[String, mutable.Set[Data]]()
    phoneNumAndData._2.map(data => {
      //use GeoHash algorithm to build neighbour relationship among bases.
      val geoHash = GeoHash.withBitPrecision(data.latitude, data.longitude, SIGNIFICANT_BITS)
      val binaryStr = getKeyFromGeohashStr(geoHash)
      if(neighbourRelationShip.contains(binaryStr)){
        val value:mutable.Set[Data] = neighbourRelationShip.get(binaryStr).get
        value += data
      }else{
        val newSet = new mutable.HashSet[Data]()
        newSet += data
        neighbourRelationShip += (geoHash.toBinaryString -> newSet)
      }
    })
    neighbourRelationShip
  }


}


object Main{
  private val phoneIndex:Int = 2
  private val baseIDIndex:Int = 9
  private val timeIndex:Int = 3
  private val longitudeIndex:Int = 4
  private val latitudeIndex:Int = 5

  private val BEGIN_TIMESTAMP = 20150000000000L
  private val END_TIMESTAMP = 20150000000000L

  def apply(): Main = new Main()
}