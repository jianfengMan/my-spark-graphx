package com.zjf.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2019-02-19 
  */
object GraphxExample1 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.ERROR)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)

    /**
      * 边有一个srcId和dstId分别对应于源和目标顶点的标示符。另外，Edge类有一个attr成员用来存储边属性。
      *
      * 我们可以分别用graph.vertices和graph.edges成员将一个图解构为相应的顶点和边。
      */

    myGraph.vertices.foreach(println(_))
    myGraph.edges.foreach(println(_))
    myGraph.triplets.foreach(println(_))
    println("--------------------------")
    myGraph.mapTriplets(t => (t.attr, t.attr == "is-friends-with" && t.srcAttr.toLowerCase.contains("a"))).triplets.foreach(println(_))
    println
    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).foreach(println(_))

    /**
      *
      * aggregateMessage函数有两个大操作，一个是sendMsg，一个是mergeMsg。aggregateMessages函数其对象是三元组。
      *
      * sendMsg是将三元组的属性信息进行转发，mergeMsg是将sendMsg转发的内容进行聚合。
      * sendMsg函数以EdgeContex作为输入参数，没返回值，提供两个消息的函数
      *
      * sendToSrc：将Msg类型的消息发送给源节点
      * sendToDst:将Msg类型消息发送给目的节点。
      *
      * 通俗理解：即以边为对象，向源节点或目的节点发送某些信息，这些信息可由aggregateMessages函数后面的mergeMsg操作来处理。
      * mergeMsg：每个顶点收到的所有信息都会被聚集起来传递给mergeMsg函数。该函数可以定义如何将信息进行转换。
      *
      * 示例：
      * 1.求图的入度
      * graph.aggregateMessages[Int](_.sendToDst(1),_+_).collect().foreach(println)
      * 2.求图的出度
      * graph.aggregateMessages[Int](_.sendToSrc(1),_+_).collect().foreach(println)
      * 解释：此处sendToDst和sendToSrc是SendMsg函数，传输值是一个整型1，_+_即mergeMsg功能函数，传递过来的数累加，发送到目的节点的所有的1的值累加即为节点的入度，发送到源节点的所有1的累加即为节点的出度。
      */

  }

}
