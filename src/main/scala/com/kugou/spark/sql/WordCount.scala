package com.kugou.spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}


//sparkSQL实现wordcount进行入门
object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()
    //添加隐式转换，toDS操作需要
    import spark.implicits._
    //读取文件生成RDD
    val lineRdd = spark.sparkContext.textFile("D:\\hadoop\\data\\stu.txt")
    //将RDD转换成DataSet，转换后执行flatMap操作将内容拆分成一个一个的单词
    val wordDS :Dataset[String] = lineRdd.toDS.flatMap(_.split(" "))
    //注册一张临时表
    wordDS.createOrReplaceTempView("words")
    //SQL按照单词分组统计每个单词出现的次数
    spark.sql("select value,count(1) from words group by value").show()
    //关闭sparkSession
    spark.stop()
  }
}
