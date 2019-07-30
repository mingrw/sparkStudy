package com.kugou.spark.core.exercise

import org.apache.spark.{SparkConf, SparkContext}

//任务描述： orderid(订单ID),userid(用户ID),payment(支付金额),productid(商品ID)（输出排名rank和payment）
object Exercise03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopN").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array("1,1768,50,155", "2,1218,600,211", "3,2239,788,242",
      "4,3101,28,599","5,4899,290,129","6,3110,54,1201", "7,4436,259,877", "8,2369,7890,27"))
    val resultRdd = rdd.map(x=>(x.split(",")(2).trim.toInt,""))//注意转换成整型进行比较
      .sortByKey(false)
      .take(5)
      .map(_._1)
    var rank = 0
    resultRdd.foreach(x=>{
      rank = rank+1
      println(rank+"\t"+x)
    })
  }
}
