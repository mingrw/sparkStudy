package com.kugou.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object StorageAPP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StorageApp").setMaster("local")
    val sc = new SparkContext(conf)
    //从hdfs中读取文件，生成lineRdd
    val lineRdd = sc.textFile("hdfs://localhost:9000/user/spark/datas/aa.txt")

    val startTime = System.currentTimeMillis()
    val resultRdd = lineRdd.count()
    val endTime = System.currentTimeMillis()

    val startTime1 = System.currentTimeMillis()
    val resultRdd1 = lineRdd.count()
    val endTime1 = System.currentTimeMillis()
    println("未加持久化策略的第一次："+(endTime-startTime)+"\t第二次"+(endTime1-startTime1))

    var lineRdd2 = sc.textFile("hdfs://localhost:9000/user/spark/datas/bb.txt")
    lineRdd2 = lineRdd2.cache()

    val startTime3 = System.currentTimeMillis()
    val resultRdd3 = lineRdd2.count()
    val endTime3 = System.currentTimeMillis()

    val startTime4 = System.currentTimeMillis()
    val resultRdd4 = lineRdd2.count()
    val endTime4 = System.currentTimeMillis()

    println("加持久化策略的第一次："+(endTime3-startTime3)+"\t第二次"+(endTime4-startTime4))
  }
}
