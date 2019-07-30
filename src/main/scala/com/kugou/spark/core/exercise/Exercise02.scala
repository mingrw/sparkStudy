package com.kugou.spark.core.exercise

import org.apache.spark.{SparkConf, SparkContext}

//题目：要求先按账户排序，在按金额排序
//    hadoop@apache          200
//    hive@apache            550
//    yarn@apache            580
//    hive@apache            159
//    hadoop@apache          300
//    hive@apache            258
//    hadoop@apache          150
//    yarn@apache            560
//    yarn@apache            260

object Exercise02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("二次排序").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(("hadoop@apache",200), ("hive@apache",550), ("yarn@apache" ,580),("hive@apache",159),
      ("hadoop@apache",300), ("hive@apache",258), ("hadoop@apache",150), ("yarn@apache",560), ("yarn@apache",260)))
    val resultRdd1 = rdd.sortBy(_._2).sortByKey()//注意此处的顺序
    resultRdd1.foreach(println)
    val resultRdd2 = rdd.groupByKey().mapValues(x=>x.toList.sortBy(y=>y))//注意此处的书写技巧，思考为何mapValues可以对单个value值操作，
                                                                        // 也可以将value值组织起来进行操作
    resultRdd2.foreach(println)
  }
}
