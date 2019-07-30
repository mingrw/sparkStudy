package com.kugou.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//文件流的作为DStream的数据输入源
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")

    //获取DStream，监控hdfs文件夹的变化情况
    val line = ssc.textFileStream("hdfs://localhost:9000/user/spark/datas/sparkstreaming")

    //编写处理逻辑
    val wordcounts = line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wordcounts.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
