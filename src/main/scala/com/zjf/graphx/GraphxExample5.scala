package com.zjf.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2019-02-19 
  */
object GraphxExample5 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.ERROR)

    val g = Graph(sc.makeRDD((1L to 7L).map((_, ""))),
      sc.makeRDD(Array(Edge(2L, 5L, ""), Edge(5L, 3L, ""), Edge(3L, 2L, ""),
        Edge(4L, 5L, ""), Edge(6L, 7L, "")))).cache
    g.connectedComponents.vertices.map(_.swap).groupByKey.map(_._2).foreach(println(_))


    // returns the userId from a file path with the format
    //   <path>/<userId>.egonet
    def extract(s: String) = {
      val Pattern = """^.*?(\d+).egonet""".r
      val Pattern(num) = s
      num
    }

    // Processes a line from an egonet file to return a
    // Array of edges in a tuple
    def get_edges_from_line(line: String): Array[(Long, Long)] = {
      val ary = line.split(":")
      val srcId = ary(0).toInt
      val dstIds = ary(1).split(" ")
      val edges = for {
        dstId <- dstIds
        if (dstId != "")
      } yield {
        (srcId.toLong, dstId.toLong)
      }
      if (edges.size > 0) edges else Array((srcId, srcId))
    }

    // Constructs Edges tuples from an egonet file
    // contents
    def make_edges(contents: String) = {
      val lines = contents.split("\n")
      val unflat = for {
        line <- lines
      } yield {
        get_edges_from_line(line)
      }
      val flat = unflat.flatten
      flat
    }

    // Constructs a graph from Edge tuples
    // and runs connectedComponents returning
    // the results as a string
    def get_circles(flat: Array[(Long, Long)]) = {
      val edges = sc.makeRDD(flat)
      val g = Graph.fromEdgeTuples(edges, 1)
      val cc = g.connectedComponents()
      cc.vertices.map(x => (x._2, Array(x._1))).
        reduceByKey((a, b) => a ++ b).
        values.map(_.mkString(" ")).collect.mkString(";")
    }

    val egonets = sc.wholeTextFiles("socialcircles/data/egonets")
    val egonet_numbers = egonets.map(x => extract(x._1)).collect
    val egonet_edges = egonets.map(x => make_edges(x._2)).collect
    val egonet_circles = egonet_edges.toList.map(x => get_circles(x))
    println("UserId,Prediction")
    val result = egonet_numbers.zip(egonet_circles).map(x => x._1 + "," + x._2)
    println(result.mkString("\n"))


  }

}
