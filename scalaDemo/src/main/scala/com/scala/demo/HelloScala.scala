package com.scala.demo

object HelloScala {

  def main(args: Array[String]): Unit = {

    println("hello,scala")

    for(i<- 1 to 9;j<- 1 to i) {
      print(s"${j}*${i}=${i*j}\t")
      if(j==1) println()
    }

  }
}
