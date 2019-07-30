package com.kugou.spark.sql.exercise

import org.apache.spark.sql.SparkSession


//spark SQL综合练习
//数据源：数据集是货品交易数据集。每个订单可能包含多个货品，每个订单可以产生多次交易，不同的货品有不同的单价。
//需求一：计算所有订单中每年的销售单数、销售总额
//需求二：计算所有订单每年最大金额订单的销售额
//需求三：计算所有订单中每年最畅销货品

object Order {
  //case class
  case class tbStock(ordernumber:String,locationid:String,dateid:String)
  case class tbStockDetail(ordernumber:String, rownum:Int, itemid:String, number:Int, price:Double, amount:Double)
  //tbdate存在的意义在于确定年，月，季度等。不太理解其意义。
  case class tbDate(dateid:String, years:Int, theyear:Int, month:Int, day:Int, weekday:Int, week:Int, quarter:Int, period:Int, halfmonth:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Order")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    //导入数据建表
    val tbStockDS = spark.sparkContext.textFile("D:\\hadoop\\data\\tbStock.txt").map(_.split(",")).map(p=>tbStock(p(0),p(1),p(2))).toDS()
    tbStockDS.createOrReplaceTempView("tbStock")
    val tbStockDetailDS = spark.sparkContext.textFile("D:\\hadoop\\data\\tbStockDetail.txt").map(_.split(","))
      .map(p=>tbStockDetail(p(0),p(1).trim.toInt,p(2),p(3).trim.toInt,p(4).trim.toDouble,p(5).trim.toDouble)).toDS()
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")
    val tbDateDS = spark.sparkContext.textFile("D:\\hadoop\\data\\tbDate.txt").map(_.split(","))
      .map(p=>tbDate(p(0),p(1).trim.toInt,p(2).trim.toInt,p(3).trim.toInt,p(4).trim.toInt,p(5).trim.toInt,p(6).trim.toInt,p(7).trim.toInt,p(8).trim.toInt,p(9).trim.toInt))
      .toDS()
    tbDateDS.createOrReplaceTempView("tbDate")

    //需求一：计算所有订单中每年的销售单数、销售总额
//    spark.sql("select theyear\n\t  ,t3.ordernumber\n\t  ,count(1) order_nums\n\t  ,sum(amount) amount_sum \nfrom\n\t (\n\t select dateid\n\t \t   ,theyear\n\t from tbDate \n\t )t1\n\t left join \n\t (\n\t select dateid\n\t \t   ,ordernumber \n\t from tbStock\n\t )t2\n\t on \n\t t1.dateid = t2.dateid\n\t left join\n\t (\n\t \tselect ordernumber\n\t \t\t  ,amount\n\t \tfrom tbStockDetail\n\t )t3\n\t on t2.ordernumber = t3.ordernumber\ngroup by t3.ordernumber,theyear")
//      .show(false)

    //需求二：计算所有订单每年最大金额订单的销售额
//    spark.sql("select theyear\n\t  ,max(amount) amount_sum \nfrom\n\t (\n\t select dateid\n\t \t   ,theyear\n\t from tbDate \n\t )t1\n\t left join \n\t (\n\t select dateid\n\t \t   ,ordernumber \n\t from tbStock\n\t )t2\n\t on \n\t t1.dateid = t2.dateid\n\t left join\n\t (\n\t \tselect ordernumber\n\t \t\t  ,sum(amount) as amount\n\t \tfrom tbStockDetail\n\t \tgroup by ordernumber\n\t )t3\n\t on t2.ordernumber = t3.ordernumber\ngroup by theyear")
//        .show()

    //需求三：计算所有订单中每年最畅销货品
    spark.sql("SELECT DISTINCT e.theyear, e.itemid, f.MaxOfAmount\nFROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount\n\tFROM tbStock a\n\t\tJOIN tbStockDetail b ON a.ordernumber = b.ordernumber\n\t\tJOIN tbDate c ON a.dateid = c.dateid\n\tGROUP BY c.theyear, b.itemid\n\t) e\n\tJOIN (SELECT d.theyear, MAX(d.SumOfAmount) AS MaxOfAmount\n\t\tFROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount\n\t\t\tFROM tbStock a\n\t\t\t\tJOIN tbStockDetail b ON a.ordernumber = b.ordernumber\n\t\t\t\tJOIN tbDate c ON a.dateid = c.dateid\n\t\t\tGROUP BY c.theyear, b.itemid\n\t\t\t) d\n\t\tGROUP BY d.theyear\n\t\t) f ON e.theyear = f.theyear\n\t\tAND e.SumOfAmount = f.MaxOfAmount\nORDER BY e.theyear")
      .show()
    spark.stop()
  }

}
