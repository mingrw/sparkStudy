package com.kugou.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/*
编写Spark应用程序，该程序对HDFS文件中的数据文件peopleinfo.txt进行统计，计算得到男性总数、女性总数、男性最高身高、女性最高身高、男性最低身高、女性最低身高。
 */
object PeopleInfo {
  def main(args: Array[String]): Unit = {
    val inPath = "hdfs://localhost:9000/user/spark/datas/peopleinfo1.txt"
    val conf = new SparkConf().setAppName("PeopleInfo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lineRDD = sc.textFile(inPath)
    val maleRdd = lineRDD.filter(_.split(" ")(1)=="M")
    val maleNum = maleRdd.foreach(println)
    val maleHighest = maleRdd.map(_.split(" ")(2)).sortBy(_.toInt,false).first()
    val maleLowest = maleRdd.map(_.split(" ")(2)).sortBy(_.toInt,true).first()
    println(maleHighest)
    println(maleLowest)
  }
}
