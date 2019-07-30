package com.kugou.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeApp {
  def main(args: Array[String]): Unit = {
    /**
      * 编写一个spark程序的步骤：1.创建SparkConf(配置spark程序的相关设置)
      */
    val conf = new SparkConf()
            .setAppName("ParallelizeApp").setMaster("local");
    val sc = new SparkContext(conf)
    val lineRdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    val result = lineRdd.reduce(_+_)
    println(result)
  }
}
