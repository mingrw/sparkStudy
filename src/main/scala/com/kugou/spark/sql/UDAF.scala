package com.kugou.spark.sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

//自定义UDAF实现分组统计每个单词出现的次数
object UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UDF")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val wordDS:Dataset[String] = spark.sparkContext.textFile("D:\\hadoop\\data\\stu.txt").toDS().flatMap(_.split(" "))
    wordDS.createOrReplaceTempView("words")

    spark.udf.register("wordcount",new WordCounts)
    spark.sql("select value,wordcount(value) from words group by value").show()
  }
}

class WordCounts extends UserDefinedAggregateFunction{
  //确定输入的类型，实际起作用的为StructField中的StringType
  override def inputSchema: StructType = {
    StructType(Array(StructField("inputType",StringType,true)))
  }
 //确定中间缓存数据的类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("inputType",IntegerType,true)))
  }
 //去顶最终输出的类型
  override def dataType: DataType = {
    IntegerType
  }
 //相同输入是否获得的是相同的输出
  override def deterministic: Boolean = true

  //初始化容器
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //聚合时当有新的值进入分组，如何操作,注意理解传入的参数的含义,说明;第一个参数表示的容器，第二个参数由inputSchema映射到的一行记录
  //input: Row对应的并非DataFrame的行
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0)+1
  }

  //全局级的合并操作(分布式导致每个节点只是聚合了处在节点的数据)，第一个参数表示容器，第二个参数表示由bufferSchema映射到的一行记录
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0)+buffer2.getInt(0)
  }

  //聚合函数返回的值
  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0)
  }
}
