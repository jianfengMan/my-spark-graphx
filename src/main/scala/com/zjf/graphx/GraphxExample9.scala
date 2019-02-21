package com.zjf.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId, lib}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description: https://blog.csdn.net/qq_34531825/article/details/52324905 这个博客不错
  * @Author: zhangjianfeng
  * @Date: Created in 2019-02-20
  */
object GraphxExample9 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.ERROR)

    // 顶点RDD[顶点的id,顶点的属性值]
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // 边RDD[起始点id,终点id，边的属性（边的标注,边的权重等）]
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // 默认（缺失）用户
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    //使用RDDs建立一个Graph
    val graph = Graph(users, relationships, defaultUser)

    val result = lib.ConnectedComponents.run(graph)
    println(result.edges)
    result.vertices.foreach(println(_))
  }
}
