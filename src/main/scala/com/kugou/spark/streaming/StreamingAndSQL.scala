package com.kugou.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming和SparkSQL结合操作
  * 案例：每隔10秒，统计最近60秒出每个商品的点击次数，然后统计出每个种类top3热门商品
  * //数据格式：name product cate 如 tom iPhone phone
  */
object StreamingAndSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachRDD").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //接收socket文本流，生成DStream,进行格式化处理
    val formatDStream = ssc.socketTextStream("localhost",9999).map(_.split(" ")).map(line=>(line(0),line(1),line(2)))
    //调用滑动窗口函数进行处理,将数据框起来
    val windowDStream = formatDStream.window(Seconds(60),Seconds(10))
    //对窗口内的数据进行指标统计并输出结果,sparkstreaming部分和sparksql部分使用相同的配置sparkconf信息
    windowDStream.transform(rdd=>{
      //获取SparkSession
      val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
      //创建DataFrame，注册成临时表
      val list = List(StructField("name",StringType,false),StructField("product",StringType,false),StructField("cate",StringType,false))
      val schema = StructType(list)
      val rowRDD = rdd.map(line=>Row(line._1,line._2,line._3))
      val df = spark.createDataFrame(rowRDD,schema)
      df.createOrReplaceTempView("products")
      //执行SQL统计相关指标
      spark.sql("select product\n\t  ,cate\n\t  ,num\nfrom\n(\n\tselect product\n\t\t  ,cate\n\t\t  ,num\n\t\t  ,row_number() over(partition by cate order by num desc) rank\n\tfrom \n\t(\n\t\tselect product\n\t\t      ,cate\n\t\t      ,count(1) num \n\t\tfrom products \n\t\tgroup by product,cate\n\t)t1\n)a\nwhere rank<=3\t").rdd
    }).print()
    ssc.start()
    ssc.awaitTermination()
  }
}

//tom iphone phone
//jack iphone phone
//tom xiaomi phone
//ming xiaomi phone
//gao huawei phone
//lucy huawei phone
//kate huawei phone
//jack oppo phone
//ming redmi phone
//a dell cp
//b apple cp
//c lenovo cp
//d asui cp
//e dell cp
//f apple cp
//g lenovo cp