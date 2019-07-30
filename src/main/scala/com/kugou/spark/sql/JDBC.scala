package com.kugou.spark.sql

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

//通过JDBC读取MySQL数据
object JDBC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JDBC")
      .master("local")
      .getOrCreate()

    //通过JDBC从MySQL中读取数据
    //定义options存储jdbc的相关信息:url,表名dbtable，用户名user，密码password
    val options1 = mutable.HashMap(("url","jdbc:mysql://localhost:3306/test"),("dbtable","people"),("user","root"),("password","root"))
    val read_df = spark.read.format("jdbc").options(options1).load()
    read_df.show()

    //通过JDBC向MySQL中写数据,***注意这种直接写的方式不支持向已存在的表中写入。***
    //构造数据
    import spark.implicits._
    val options2 = mutable.HashMap(("url","jdbc:mysql://localhost:3306/test"),("dbtable","person"),("user","root"),("password","root"))
    val write_df = spark.sparkContext.parallelize(Array(("ming",20),("gao",34))).toDF("name","age")
    write_df.write.format("jdbc").options(options2).save()

  }
}
