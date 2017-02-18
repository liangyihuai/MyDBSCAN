import java.io._
import java.util.Scanner

import scala.collection.immutable.TreeMap
import scala.io.Source

/**
  * Created by liangyh on 1/8/17.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val list = List(1,5,3,2,6,9,7)
    val list2 = list.sortWith((a, b)=> a>b)
    println(list2)
  }

}
