package com.zjf.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2019-02-19
  */
object GraphxExample7 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.ERROR)
    //
    //    val edges = sc.makeRDD(Array(
    //      Edge(1L, 11L, 5.0), Edge(1L, 12L, 4.0), Edge(2L, 12L, 5.0),
    //      Edge(2L, 13L, 5.0), Edge(3L, 11L, 5.0), Edge(3L, 13L, 2.0),
    //      Edge(4L, 11L, 4.0), Edge(4L, 12L, 4.0)))
    //
    //    val conf = new lib.SVDPlusPlus.Conf(2, 10, 0, 5, 0.007, 0.007, 0.005, 0.015)
    //
    //    val (g, mean) = lib.SVDPlusPlus.run(edges, conf)
    //    println(g,mean)

    val vertices = sc.makeRDD(Seq(
      (1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Dianne")))
    val edges = sc.makeRDD(Seq(
      Edge(1L, 2L, "is-friends-with"), Edge(1L, 3L, "is-friends-with"),
      Edge(4L, 1L, "has-blocked"), Edge(2L, 3L, "has-blocked"),
      Edge(3L, 4L, "has-blocked")))
    val originalGraph = Graph(vertices, edges)
    val subgraph = originalGraph.subgraph(et => et.attr == "is-friends-with")

    // show vertices of subgraph � includes Dianne
    subgraph.vertices.foreach(println)

    // now call removeSingletons and show the resulting vertices
    removeSingletons(subgraph).vertices.foreach(println)

    def removeSingletons[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) =
      Graph(g.triplets.map(et => (et.srcId, et.srcAttr))
        .union(g.triplets.map(et => (et.dstId, et.dstAttr)))
        .distinct,
        g.edges)


//    val rdd = sc.makeRDD(1 to 10000)
    //    rdd
    //      .filter(_ % 4 == 0)
    //      .map(Math.sqrt(_))
    //      .map(el => (el.toInt, el))
    //      .groupByKey
    //      .foreach(println(_))

    val iterations = 500
    var g = Graph.fromEdges(sc.makeRDD(
      Seq(Edge(1L, 3L, 1), Edge(2L, 4L, 1), Edge(3L, 4L, 1))), 1)
    for (i <- 1 to iterations) {
      println("Iteration: " + i)
      val newGraph: Graph[Int, Int] =
        g.mapVertices((vid, vd) => (vd * i) / 17)
      g = g.outerJoinVertices[Int, Int](newGraph.vertices) {
        (vid, vd, newData) => newData.getOrElse(0)
      }
    }
    g.vertices.collect.foreach(println)


  }
}
