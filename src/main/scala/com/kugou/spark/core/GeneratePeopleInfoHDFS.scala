package com.kugou.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/*
    生成随机数据集，格式如下：序号 性别 身高
                              1    F    170
                              2    M    178
                              3    M    174
                              4    F    165
     */
object GeneratePeopleInfoHDFS {

  //定义一个方法用于随机生成性别
  def getRandomGender(): String ={
    val rand = new Random()
    val gender = rand.nextInt(2)
    if(gender==1) "M" else "F"
  }



  def main(args: Array[String]): Unit = {
    val outputFile = "hdfs://localhost:9000/user/spark/datas/peopleinfo1.txt"
    //创建sparkConf，配置相关信息，利用sparkConf创建sparkContext对象。
    val conf = new SparkConf().setAppName("GeneratePeopleInfoHDFS").setMaster("local")
    val sc = new SparkContext(conf)
    val rand = new Random()
    //声明字符串数组存储生成的数据集，数据集行数为10000行
    val arr = new Array[String](10000)
    for(i<-0 until 10000){
      val gender = getRandomGender()
      var height = rand.nextInt(230)
      //将身高不符合逻辑的进行修改
      if(height<50){
        height += 120
      }
      if(height<150 && gender== "M"){
        height += 40
      }else if(height<150 && gender == "F"){
        height += 30
      }
      arr(i) = (i+1) +" "+gender+" "+height
    }
    val rdd = sc.parallelize(arr)
    rdd.foreach(println)
    rdd.saveAsTextFile(outputFile)
  }
}
