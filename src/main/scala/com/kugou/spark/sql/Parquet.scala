package com.kugou.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

//读写Parquet格式文件
object Parquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("parquet")
      .master("local[*]")
      .getOrCreate()

    //读取parquet文件
   // val df = spark.read.parquet("D:\\hadoop\\data\\users.parquet")
    val rdd = spark.read.format("parquet").load("D:\\hadoop\\data\\users.parquet").toDF("kame","color","num").rdd
    import spark.implicits._
    //两种方式：
    val rowRdd1 = rdd.map(row=>Row(row.getString(0),row.getString(1)))
    val schema = StructType(Array(StructField("name",StringType,false),StructField("color",StringType,true)))
    val df = spark.createDataFrame(rowRdd1,schema)


    //写parquet格式文件,***注意save需要提供的是一个文件夹的路径***
    df.write.mode("append").format("parquet").save("D:\\hadoop\\data\\users")
    //df.write.mode("append").parquet("")
    df.show()
    /*
    +------+-----+
    |  name|color|
    +------+-----+
    |Alyssa| null|
    |   Ben|  red|
    +------+-----+
     */
    val df1 = spark.read.format("parquet").load("D:\\hadoop\\data\\users.parquet")
    df1.show()
    /*
    +------+--------------+----------------+
    |  name|favorite_color|favorite_numbers|
    +------+--------------+----------------+
    |Alyssa|          null|  [3, 9, 15, 20]|
    |   Ben|           red|              []|
    +------+--------------+----------------+
     */
    //测试parquet格式文件的schema的合并功能
    //合并功能的说明：
    val df2 = spark.read.option("mergeSchema",true).parquet("D:\\hadoop\\data\\users")
    df2.printSchema()
    df2.show()
    /*不加schema合并,
    +------+-----+
    |  name|color|
    +------+-----+
    |Alyssa| null|
    |   Ben| null|
    |Alyssa| null|
    |   Ben|  red|
    +------+-----+
    加了mergeSchema
    +------+-----+--------------+----------------+
    |  name|color|favorite_color|favorite_numbers|
    +------+-----+--------------+----------------+
    |Alyssa| null|          null|  [3, 9, 15, 20]|
    |   Ben| null|           red|              []|
    |Alyssa| null|          null|            null|
    |   Ben|  red|          null|            null|
    +------+-----+--------------+----------------+
     */

    spark.stop()
  }
}
