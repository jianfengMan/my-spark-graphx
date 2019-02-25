package com.zjf.graphx.learning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConnectedComponents").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load the edges as a graph
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "/Users/zhangjianfeng/workspaces/workspace_github_bg/my-spark-graphx/data/followers.txt")

    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val users = sc.textFile("/Users/zhangjianfeng/workspaces/workspace_github_bg/my-spark-graphx/data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result

    println("\ngraph edges");
    println("edges:");
    graph.edges.collect.foreach(println)
    graph.edges.collect.foreach(println)
    println("vertices:");
    graph.vertices.collect.foreach(println)
    println("triplets:");
    graph.triplets.collect.foreach(println)
    println("\nusers");
    users.collect.foreach(println)
    println("\ncc:");
    cc.collect.foreach(println)
    println("\nccByUsername");
    println(ccByUsername.collect().mkString("\n"))
  }
}
