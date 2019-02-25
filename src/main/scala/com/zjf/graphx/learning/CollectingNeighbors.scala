package com.zjf.graphx.learning

import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description:收集每个顶点的邻居顶点
  * @Author: zhangjianfeng
  * @Date: Created in 2019-02-25
  */
object CollectingNeighbors {
  val K = 3
  var arr = new Array[(Int, Int)](K)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollectingNeighbors").setMaster("local[4]")
    // Assume the SparkContext has already been constructed
    val sc = new SparkContext(conf)

    // Import random graph generation library
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 6).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age

    println("Graph:");
    println("sc.defaultParallelism:" + sc.defaultParallelism);
    println("vertices:");
    graph.vertices.collect.foreach(println(_))
    println("edges:");
    graph.edges.collect.foreach(println(_))
    println("------------------1--------------------")
    println("count:" + graph.edges.count);
    println("\ninDegrees");
    graph.inDegrees.foreach(println)
    println("------------------2--------------------")

    println("\nneighbors0:");
    val neighbors0 = graph.collectNeighborIds(EdgeDirection.Out)
    neighbors0.foreach(println)
    neighbors0.collect.foreach { a => {
      println(a._1 + ":")
      a._2.foreach(b => print(b + " "))
      println();
    }
    }

  }
}
