package com.kugou.spark.core.exercise

import org.apache.spark.{SparkConf, SparkContext}
import util.control.Breaks._

//topN问题  求每个班级的前三名
object Exercise06 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("E06").setMaster("local")
    val sc = new SparkContext(conf)
    val lineRdd = sc.parallelize(Array(("class01",98),("class02",88),("class01",78),("class01",88),("class02",99),("class02",77),("class01",57),("class02",77)))
    //思路：分组后取前三====>groupByKey会按照key进行分组，value存放在迭代器中。取前三可以声明一个数组，只需遍历一次即可，O(n)的时间复杂度
    val resultRdd = lineRdd.groupByKey()
      .foreach(line=>{
        print("班级为："+line._1)
        val list = new Array[Int](3)
        line._2.foreach(value=>{
          //否则和数组元素进行比较
            breakable {
              for (i <- 0 until 3) {
                if (value > list(0)) {
                  list(2) = list(1)
                  list(1) = list(0)
                  list(0) = value
                  break()
                } else if (value > list(1)) {
                  list(2) = list(1)
                  list(1) = value
                  break()
                } else if (value > list(2)) {
                  list(2) = value
                  break()
                }
              }
            }
        })
        println("\t前三名成绩为："+list(0)+","+list(1)+","+list(2))
      })
  }
}
