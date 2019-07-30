package com.kugou.spark.streaming

import java.sql.{Connection, DriverManager}
import java.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 对foreachRDD算子进行相关实践操作。
  * 案例：将单词统计的结果写入到MySQL数据库,使用数据库连接池
  */
object ForeachRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachRDD").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //接收socket文本流，生成DStream，进行词频统计
    val line = ssc.socketTextStream("localhost",9999)
    val wordCounts = line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //将统计的结果写入到MySQL数据库，写入的过程是一个output操作，可调用foeeachRDD操作进行,
    // 倘若对每个RDD创建一个Connection，过于浪费资源，所以使用foreachPartition操作对每个分区分配一个
    wordCounts.foreachRDD(rdd=>{
      //val conn = ConnectionPool.getConnection()  在此处创建不可以，因为此处创建的话，对于rdd的action操作来说，使用的是外部变量，需要进行序列化，
      // 但是Connection对象无法被序列化
      rdd.foreachPartition(itearator=>{
        val conn = ConnectionPool.getConnection()
        val preparedStatement = conn.prepareStatement("insert into wordcounts(word,count) values(?,?)")
        for(value<-itearator){
          preparedStatement.setString(1,value._1)
          preparedStatement.setInt(2,value._2)
          preparedStatement.addBatch()
        }
        preparedStatement.executeBatch()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

//使用懒汉式实现一个简易版的数据库连接池
object  ConnectionPool{
  var pool : util.LinkedList[Connection] = null
  val url = "jdbc:mysql://localhost:3306/test?useSSL=false"
  val name = "root"
  val password = "root"
  //加载驱动
  Class.forName("com.mysql.jdbc.Driver")
  //获取连接
  def getConnection():Connection = {
    var conn:Connection = null
    //获取连接时先判断,没有连接了再创建
      if(pool==null) {
        conn = DriverManager.getConnection(url, name, password)
      }
      else{
        conn = pool.pop()
      }
    conn
  }

  //回收连接
  def returnConnection(conn:Connection)={
    pool.push(conn)
  }
}