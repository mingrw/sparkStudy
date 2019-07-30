package com.kugou.spark.core.exercise

import org.apache.spark.{SparkConf, SparkContext}

//通过wordcount的结果进行排序,使用sortBy即可
object Exercise04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Exercise04").setMaster("local")
    val sc = new SparkContext(conf)
    val lineRdd  = sc.parallelize(Array("hello me","hello you","hello world","hello kitty","me java","java spark hadoop python scala"))
    val resultRdd = lineRdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false)
    resultRdd.saveAsTextFile("hdfs://localhost:9000/user/spark/datas/wordcount.txt")
    resultRdd.foreach(println)
  }
}
