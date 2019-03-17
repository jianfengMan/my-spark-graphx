package com.zjf.graphx.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * @Description
  * @Author zhangjianfeng
  * @Date 2019-03-13
  **/
object Simple {
  def main(args: Array[String]) {
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
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // 定义默认的作者,以防与不存在的作者有relationship边
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    /** *********************************
      * 展示图的属性
      * **********************************/
    println("属性展示")
    println("---------------------------------------------")
    println("找到图中属性是student的顶点")
    graph.vertices.filter { case (id, (name, occupation)) => occupation == "student" }.collect.foreach {
      case (id, (name, occupation)) => println(s"$name is $occupation")
    }
    println("---------------------------------------------")
    println("找到图中边属性是advisor的边")
    graph.edges.filter(e => e.attr == "advisor").collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println("---------------------------------------------")
    println("找出图中最大的出度、入度、度数：")

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))

    /***********************************
      *展示结构操作
      ***********************************/
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    println("---------------------------------------------")
    println("删除不存在的节点，构建子图")
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    println("---------------------------------------------")
    println("构建职业是professor的子图,并打印子图的顶点")
    val subGraph = graph.subgraph(vpred = (id, attr) => attr._2 == "prof")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
  }
}
