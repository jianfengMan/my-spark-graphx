package com.zjf.graphx.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

object AgreegateTest {

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
    //设置users顶点
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    //设置relationships边
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(Array(Edge(3L, 7L, 1.1), Edge(5L, 3L, 1.1), Edge(2L, 5L, 1.1), Edge(5L, 7L, 1.1), Edge(6L, 8L, 1.1)))
    // 定义默认的作者,以防与不存在的作者有relationship边
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        // Send message to destination vertex containing counter and age
        triplet.sendToDst(1, triplet.attr)
        triplet.sendToSrc(1, triplet.attr)
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers

    graph.edges.collect.foreach(println(_))
    println("count:" + graph.edges.count);
    println("\nolderFollowers:");
    olderFollowers.collect.foreach(println)
    println("\navgAgeOfOlderFollowers:")

    println("-------------------------------------------------")
    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => id != 5)
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph)
    println("-------------------------------------------------")
    ccGraph.triplets.foreach(println)
    println("-------------------------------------------------")
    validGraph.triplets.foreach(println)
    println("-------------------------------------------------")
    validCCGraph.triplets.foreach(println)
    println("-------------------------------------------------")
    validGraph.connectedComponents().triplets.foreach(println(_))

    val ss = validCCGraph.triplets.map(triplet => (triplet.srcAttr, Edge(triplet.srcId, triplet.dstId, triplet.attr)))
      .groupByKey().sortBy(_._2.size, false).take(1)(0)
    println(ss._2)

    var ite = List(1, 2, 3)
    while (ite.size != 0) {
      ite = testMethon(ite)
    }


  }

  def testMethon(ite: List[Int]) = {
    var tempIte = ite
    import scala.util.control.Breaks._
    breakable {
      for (id <- ite) {
        println(id)
        if (id == 2) {
          tempIte = List[Int]()
          break()
        }

      }
    }
    tempIte
  }

}
