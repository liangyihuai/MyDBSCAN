/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package test


import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.dbscan.{DBSCAN, RawVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  *  /home/liangyh/installed/spark/bin/spark-submit --class test.SampleDBSCANJob ~/IdeaProjects/MyDBSCAN/out/artifacts/MyDBSCAN_jar/MyDBSCAN.jar
  *
  *
  * spark-submit ./MyDBSCAN.jar hdfs://cloudera2:8020/user/cloudera/dbscanInput.txt hdfs://cloudera2:8020/user/cloudera/output
  *
  * spark-submit --master yarn-cluster --num-executors 3 ./MyDBSCAN.jar  hdfs://cloudera2:8020/user/cloudera/dbscanInput.txt hdfs://cloudera2:8020/user/cloudera/output
  *  scp root@172.33.27.179:/home/liangyh//IdeaProjects/MyDBSCAN/out/artifacts/MyDBSCAN_jar/MyDBSCAN.jar ./
  *
  *  scp root@172.33.27.179:/home/liangyh/Documents/GRADUATE-PROJECT/DataSet/scala-compiler-2.11.0.jar ./
  */
object SampleDBSCANJob{

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args:Array[String]):Unit = {

//    val input:String = "hdfs://127.0.0.1:9000/user/liangyh/twoDemension.txt";
//    val input:String = "/home/liangyh/Documents/GRADUATE-PROJECT/DataSet/part.txt"
//    val input:String = "/home/liangyh/Documents/GRADUATE-PROJECT/DataSet/AGG_3G_BSSAP_UTF8.txt"
//    val output:String = "hdfs://127.0.0.1:9000/user/liangyh/output";

    if(args.length < 2){
      logger.warn("Usage: ./spark-submit jarPath  inputPath outputPath")
      System.exit(0)
    }
    val input:String = args(0)
    val output:String = args(1)

    val maxPointsPerPartition:Int = 500000
    val eps = 0.00528
    val minPoints = 3;

//    val hdfs = org.apache.hadoop.fs.FileSystem.get(
//      new java.net.URI("hdfs://127.0.0.1:9000"), new org.apache.hadoop.conf.Configuration())
//
//    val path = new Path(output);
//    if(hdfs.exists(path)){
//      hdfs.delete(path, true)
//    }

    val conf = new SparkConf()
      .setAppName(s"DBSCAN(eps=$eps, min=$minPoints, max=$maxPointsPerPartition) -> $output")
    //conf.setMaster("spark://cloudera1:7077");

    val sc = new SparkContext(conf)
//    sc.addJar("/home/liangyh/IdeaProjects/MyDBSCAN/out/artifacts/MyDBSCAN_jar/MyDBSCAN.jar");

    val data = sc.textFile(input)

    logger.info("----------------begin---------------")

    def parseLine(str: String): RawVector = {
      val line = str.split('|')
      val time = line(4)
      val longitude = line(line.size - 2)
      val latitude = line(line.size - 1)
      val vector = Vectors.dense(longitude.toDouble, latitude.toDouble)
      RawVector(vector, time)
    }

    val parsedData = data
      .filter(s => {!s.isEmpty && s.charAt(s.length-1) != 'N'})
      .map(parseLine)
      .cache()

    logger.info("--------------------begin to train------------------------")
    val model = DBSCAN.train(
      parsedData,
      eps = eps,
      minPoints = minPoints,
      maxPointsPerPartition = maxPointsPerPartition)

    logger.info("--------------------finish train------------------")
    model.labeledPoints.map(p => s"${p.x},${p.y},${p.attachment},${p.cluster},${p.flag}").saveAsTextFile(output)

    sc.stop()
    logger.info("--------stop-------")
  }
}