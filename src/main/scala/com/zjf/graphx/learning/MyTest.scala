package com.zjf.graphx.learning

object MyTest {

  def main(args: Array[String]): Unit = {
    println(this.getClass().getSimpleName().filter(!_.equals('$')))
  }

}
