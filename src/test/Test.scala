import java.io._
import java.util.Scanner

import scala.collection.immutable.TreeMap
import scala.io.Source

/**
  * Created by liangyh on 1/8/17.
  */
object Test {
  private val data = "A1LOC|2|69AE7460C4A10832AA75FE78F57AA11B|20160923222132|20160923222132|广东|200|\\N|\\N|\\N|\\N|null|null|1338D6F5CE8F587FBB8170F8FDF323A1|china|null|C81|5EF1|160923222132|0-9-16|0-8-10||||1|0|16|9|2016092322|\\N|\\N\nA1CALL|0|213B06883BC5ECAC0DB5909D589947B2|20160923180752|20160923180815|广东|200|\\N|\\N|\\N|\\N|null|null|863C241141F6C53ADF57DFDF9EB15575|china|null|981|4A91|160923180815|0-8-161|0-8-19|2016-09-23 18:08:01.000000|2016-09-23 18:07:55.000000|2016-09-23 18:08:15.000000|5|0|16|14|2016092318|\\N|\\N\nA1LOC|0|39FCAF26A2E018AA67917A3CF5B09740|20160923210843|20160923210843|广东|200|\\N|\\N|\\N|\\N|null|null|73A724CA10FA64D24395283383787ACB|china|null|8C1|1054|160923210843|0-8-115|0-8-10||||1|0|16|9|2016092321|\\N|\\N\nA1LOC|0|61A4E39BC677C19351077A1ED606F7E1|20160923130316|20160923130317|广东|200|762|0|3|1|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3247|160923130317|0-12-137|0-12-129|||2016-09-23 13:03:16.000000|2|0|16|9|2016092313|114.213|24.0563\nA1LOC|0|61A4E39BC677C19351077A1ED606F7E1|20160923130316|20160923130317|广东|200|762|0|3|1|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3247|160923130317|0-12-137|0-12-129|||2016-09-23 13:03:16.000000|2|0|16|9|2016092313|113.0751|23.72015\nA1LOC|0|61A4E39BC677C19351077A1ED606F7E1|20160923130316|20160923130317|广东|200|762|0|3|1|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3247|160923130317|0-12-137|0-12-129|||2016-09-23 13:03:16.000000|2|0|16|9|2016092313|116.7202|23.35707\nA1LOC|0|61A4E39BC677C19351077A1ED606F7E1|20160923130316|20160923130317|广东|200|762|0|3|1|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3247|160923130317|0-12-137|0-12-129|||2016-09-23 13:03:16.000000|2|0|16|9|2016092313|116.0976|24.30042\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132639|20160923132639|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132639|0-12-137|0-12-129|2016-09-23 13:26:39.000000|||0|0|16|9|2016092313|113.0751|23.72015\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132639|20160923132639|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132639|0-12-137|0-12-129|2016-09-23 13:26:39.000000|||0|0|16|9|2016092313|116.7202|23.35707\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132639|20160923132639|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132639|0-12-137|0-12-129|2016-09-23 13:26:39.000000|||0|0|16|9|2016092313|116.0976|24.30042\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132639|20160923132639|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132639|0-12-137|0-12-129|2016-09-23 13:26:39.000000|||0|0|16|9|2016092313|114.213|24.0563\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132639|20160923132639|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132639|0-12-137|0-12-129|2016-09-23 13:26:39.000000|||0|0|16|9|2016092313|114.7012|23.7503\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132628|20160923132629|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132629|0-12-137|0-12-129|2016-09-23 13:26:29.000000|||0|0|16|9|2016092313|113.0751|23.72015\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132628|20160923132629|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132629|0-12-137|0-12-129|2016-09-23 13:26:29.000000|||0|0|16|9|2016092313|116.7202|23.35707\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132628|20160923132629|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132629|0-12-137|0-12-129|2016-09-23 13:26:29.000000|||0|0|16|9|2016092313|116.0976|24.30042\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132628|20160923132629|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132629|0-12-137|0-12-129|2016-09-23 13:26:29.000000|||0|0|16|9|2016092313|114.213|24.0563\nA1SMS|0|61A4E39BC677C19351077A1ED606F7E1|20160923132628|20160923132629|广东|200|762|0|3|2|null|null|B199A87130F2533E1B98EFA32E36AE15|china|河源华怡L|9001|3248|160923132629|0-12-137|0-12-129|2016-09-23 13:26:29.000000|||0|0|16|9|2016092313|114.7012|23.7503\nA1LOC|0|68F12A17DDB815E10E00DF7B55A1713D|20160923143715|20160923143715|广东|200|751|0|5|0|null|null|A562884EF78EAC901E2673DB2CCE141B|china|广东省韶关市曲江区新华书店联通基站|9801|58DB|160923143715|0-11-72|0-11-65|||2016-09-23 14:37:15.000000|2|0|16|9|2016092314|115.1809|23.63805\nA1LOC|0|68F12A17DDB815E10E00DF7B55A1713D|20160923143715|20160923143715|广东|200|751|0|5|0|null|null|A562884EF78EAC901E2673DB2CCE141B|china|广东省韶关市曲江区新华书店联通基站|9801|58DB|160923143715|0-11-72|0-11-65|||2016-09-23 14:37:15.000000|2|0|16|9|2016092314|116.1101|24.31243"

  def main(args: Array[String]): Unit = {
    val file = new File("/home/liangyh/tmp/output");
    val files = file.listFiles()
    val filtered = files.filter(file => file.isFile() && file.length() > 0)

    var map = new TreeMap[Int, String]

    /*val mapped = filtered.map(file => {
      for(line <- Source.fromFile(file).getLines()){
        val index = line.indexOf('-')
        val key:Int = line.substring(index + 1).trim.toInt
        var value:String = line.substring(0, index-1)
        if(map.contains(key)){
          value = map.get(key).get + "|"+value
        }
        map += (key -> value)
      }
    })*/

    val mapped = filtered.map(file => {
      val reader = new BufferedReader(new FileReader(file))

      var line = reader.readLine()
      while(line != null){
        val index = line.indexOf('-')
        val key:Int = line.substring(index + 1).trim.toInt
        var value:String = line.substring(0, index-1)
        if(map.contains(key)){
          value = map.get(key).get + "|"+value
        }
        map += (key -> value)

        line = reader.readLine()
      }

/*      for(line <- Source.fromFile(file).getLines()){
        val index = line.indexOf('-')
        val key:Int = line.substring(index + 1).trim.toInt
        var value:String = line.substring(0, index-1)
        if(map.contains(key)){
          value = map.get(key).get + "|"+value
        }
        map += (key -> value)
      }*/
    })

    val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/liangyh/tmp/output.txt")))

    map.foreach(entry => (out.write(entry._1 +"-" + entry._2+"\n")))

    out.close()

  }

  def filterTest(): Unit = {
    data
      .split('\n')
      .filter(s => (!s.isEmpty && s.charAt(s.length-1) != 'N'))
      .map(str => {
        val line = str.split('|')
        println(line(line.size-2)+";"+line(line.size-1))
      })
  }

  def splitTest():Unit = {
    val str = "我,是谁？"

    for(index <- 0 until(str.length)){
      println(str.charAt(index))
    }
  }

}
