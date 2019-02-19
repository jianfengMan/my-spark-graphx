package com.zjf.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId, lib}
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2019-02-19 
  */
object GraphxExample3 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.ERROR)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)
    println()


    def toGexf[VD, ED](g: Graph[VD, ED]) =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
        "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
        "    <nodes>\n" +
        g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
          v._2 + "\" />\n").collect.mkString +
        "    </nodes>\n" +
        "    <edges>\n" +
        g.edges.map(e => "      <edge source=\"" + e.srcId +
          "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
          "\" />\n").collect.mkString +
        "    </edges>\n" +
        "  </graph>\n" +
        "</gexf>"

    //    val pw = new java.io.PrintWriter("myGraph.gexf")
    //    pw.write(toGexf(myGraph))
    //    pw.close

    myGraph.vertices.map(x => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(
        com.fasterxml.jackson.module.scala.DefaultScalaModule)
      val writer = new java.io.StringWriter()
      mapper.writeValue(writer, x)
      writer.toString
    }).coalesce(1, true).saveAsTextFile("myGraphVertices")



  }

}
