package com.zjf.graphx.learning

import org.apache.spark.graphx.{GraphLoader, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

object WebGoogle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConnectedComponents").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Parse the edge data which is already in userId -> userId format
    val graph = GraphLoader.edgeListFile(sc, "/Users/zhangjianfeng/workspaces/workspace_github_bg/my-spark-graphx/data/graphx-wiki-edges.txt")
    println("graph.numEdges:" + graph.numEdges);
    println("graph.numVertices:" + graph.numVertices);
    println("\n edges 10:");
    graph.edges.take(10).foreach(println);
    println("\n vertices 10:");
    graph.vertices.take(10).foreach(println);

    //***************************************************************************************************
    //*******************************          图的属性          *****************************************
    //***************************************************************************************************
    println("**********************************************************")
    println("属性演示")
    println("**********************************************************")
    println("Graph:");

    //Degrees操作
    println("找出图中最大的出度、入度、度数：")

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))
    println

    sc.stop
  }
}
