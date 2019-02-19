package com.zjf.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2019-02-19
  */
object GraphxExample8 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.ERROR)
    sc.setCheckpointDir("spark-checkpoint")
    var updateCount = 0
    val checkpointInterval = 50

    def update(newData: Graph[Int, Int]): Unit = {
      newData.persist()
      updateCount += 1
      if (updateCount % checkpointInterval == 0) {
        newData.checkpoint()
      }
    }

    val iterations = 500
    var g = Graph.fromEdges(sc.makeRDD(
      Seq(Edge(1L, 3L, 1), Edge(2L, 4L, 1), Edge(3L, 4L, 1))), 1)
    update(g)
    g.vertices.count
    for (i <- 1 to iterations) {
      println("Iteration: " + i)
      val newGraph: Graph[Int, Int] = g.mapVertices((vid, vd) => (vd * i) / 17)
      g = g.outerJoinVertices[Int, Int](newGraph.vertices) {
        (vid, vd, newData) => newData.getOrElse(0)
      }
      update(g)
      g.vertices.count
    }
    g.vertices.collect.foreach(println)


  }
}
