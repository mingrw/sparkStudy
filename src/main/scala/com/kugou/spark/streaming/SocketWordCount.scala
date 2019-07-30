package com.kugou.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//基于socket数据源得spark streaming版的得wordcount程序
object SocketWordCount {
  def main(args: Array[String]): Unit = {
    //配置StreamingContext对象
    val conf = new SparkConf()
      .setAppName("SocketWordCount")
      .setMaster("local[2]") //注意cpu core的核心数的设置，因为需要一个core用于接收数据，所以至少设置为2
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")

    //获取DStream   以文本形式从套接字流中获取，设置套接字流的ip和端口号
    val line = ssc.socketTextStream("localhost",9999)
    //编写rdd程序处理逻辑
    val wordcounts = line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wordcounts.print()

    //固定启动
    ssc.start()
    ssc.awaitTermination()
  }
}
