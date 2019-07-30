package com.kugou.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args:Array[String]): Unit = {
    val logFile="D:\\hadoop\\data\\stu.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lineRdd = sc.textFile(logFile,1)
    val wordsRdd = lineRdd.flatMap(_.split(" ")).map((_,1))
    val resultRdd = wordsRdd.reduceByKey(_+_)
    resultRdd.foreach(println)
  }

}
