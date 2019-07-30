package com.kugou.spark.core.exercise

import org.apache.spark.{SparkConf, SparkContext}

//题目：给定一组键值对("spark",2),("hadoop",6),("hadoop",4),("spark",6)，键值对的key
//            表示图书名称，value表示某天图书销量，请计算每个键对应的平均值，也就是计算每种图书的每天平均销量。
object Exercise01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("求均值").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(("spark",2),("hadoop",6),("hadoop",4),("spark",6)))
    val resultRdd = rdd.mapValues(x=>(x,1))//下划线的使用场景为弄透彻。。。。
                        .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
                        .mapValues(x=>(x._1/x._2))
    resultRdd.foreach(println)
    //总结：练习了关于key-value的transforaction操作
  }
}
