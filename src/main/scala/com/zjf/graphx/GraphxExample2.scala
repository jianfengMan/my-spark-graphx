package com.zjf.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2019-02-19 
  */
object GraphxExample2 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.ERROR)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)
    myGraph.edges.foreach(println(_))
    println()


    //UserJoin
    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).join(myGraph.vertices).foreach(println)

    println()
    //UserMapAndSwap
    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).join(myGraph.vertices).map(_._2.swap).foreach(println)
    //    swap 交换key和value的值

    println()
    //UserJoin
    myGraph.aggregateMessages[Int](_.sendToSrc(2),
      _ + _).join(myGraph.vertices).foreach(println)

    println()
    //UserMapAndSwap
    myGraph.aggregateMessages[Int](_.sendToSrc(2),
      _ + _).join(myGraph.vertices).map(_._2.swap).foreach(println)
    //    swap 交换key和value的值


    /**
      * (3,(2,Charles))
      * (1,(1,Ann))
      * (4,(1,Diane))
      * (2,(1,Bill))
      *
      * (Diane,1)
      * (Bill,1)
      * (Ann,1)
      * (Charles,2)
      *
      * (4,(2,Diane))
      * (2,(2,Bill))
      * (3,(4,Charles))
      * (1,(2,Ann))
      *
      * (Ann,2)
      * (Charles,4)
      * (Diane,2)
      * (Bill,2)
      */

    println()
    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).rightOuterJoin(myGraph.vertices).map(_._2.swap).foreach(println(_))

    println()
    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).rightOuterJoin(myGraph.vertices).map(
      x => (x._2._2, x._2._1.getOrElse(0))).foreach(println(_))

    println()
    myGraph.vertices.saveAsObjectFile("myGraphVertices")
    myGraph.edges.saveAsObjectFile("myGraphEdges")
    val myGraph2 = Graph(
      sc.objectFile[Tuple2[VertexId, String]]("myGraphVertices"),
      sc.objectFile[Edge[String]]("myGraphEdges"))


  }

}
