package com.kugou.spark.sql

import org.apache.spark.sql.{Row, functions}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ DataType, StringType, StructField, StructType}


//自定义UDAF实现max函数,如何适用于多种数据类型(如IntegerType，DoubleType，StringType等)
object UDAF02 {
  def main(args: Array[String]): Unit = {
    functions
  }
}

class Max extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    StructType(Array(StructField("num",StringType,true)))
  }

  override def bufferSchema: StructType = ???

  override def dataType: DataType = ???

  override def deterministic: Boolean = ???

  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}
