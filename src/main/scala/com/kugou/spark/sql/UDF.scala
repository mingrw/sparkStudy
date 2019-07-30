package com.kugou.spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}

//统计各个单词的长度
object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UDF")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val wordDS:Dataset[String] = spark.sparkContext.textFile("D:\\hadoop\\data\\stu.txt").toDS().flatMap(_.split(" "))
    wordDS.createOrReplaceTempView("words")

    //注册UDF函数，指定函数名，传入一个函数(封装执行逻辑)
    spark.udf.register("length",(str:String)=>str.length)
    //注册后可直接使用
    spark.sql("select value,length(value) from words").show()
    spark.stop()
  }
}
