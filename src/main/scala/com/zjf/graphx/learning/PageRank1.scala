package com.zjf.graphx.learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PageRank1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRank").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()


    val edges = sc.makeRDD(Array((1L, 2L, 10), (1L, 4L, 1),
      (2L, 3L, 1), (2L, 4L, 1), (3L, 4L, 1), (4L, 2L, 1), (4L, 1L, 1)))

    //Define alpha
    val alpha = 0.85
    val iterCnt = 40
    val links = edges.map(x => (x._1, x._2)).groupByKey()
    var ranks = links.map(x => (x._1, 1.0))
    var result = Map[Long, Double]()
    edges.map(x => (x._2, x._3)).reduceByKey(_ + _).collect().foreach(x => {
      result += (x._1 -> x._2)
    })

    result.foreach(println(_))
    //Iteration
    for (i <- 0 until iterCnt) {
      val contributions = links.join(ranks).flatMap {
        case (srdId, (linkList, rank)) =>
          linkList.map(dest => (dest, rank / result.getOrElse(srdId, 1.0)))
      }
      ranks = contributions.reduceByKey((x, y) => x + y)
        .mapValues(v => {
          (1 - alpha) + alpha * v
        })
    }

    /**
      * (1,0.7806983716879278)
      * (2,1.112495148235923)
      * (3,0.6228104222905804)
      * (4,1.483996057785569)
      */

    ranks.sortByKey().collect().foreach(println)
  }
}
