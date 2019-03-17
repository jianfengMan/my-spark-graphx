package com.zjf.graphx.learning


object ListTest {

  def main(args: Array[String]): Unit = {
    var list = List[Double](1.0, 2.0)
    while (list.size != 0) {
      println(list.size)
      val topValue = list(0)
      println(topValue)
      list = list.filter(_ != topValue)
    }
  }

}
