package com.kugou.spark.sql

import org.apache.spark.sql.SparkSession

//对spark SQL的开窗函数进行测试
object WindowsFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WindowsFunction")
      .master("local")
      .getOrCreate()
    //构造一些数据用以测试
    import spark.implicits._
    val rowRDD = spark.sparkContext
      .parallelize(Array((1,23,"2019-06-21"),(1,45,"2019-07-12"),(1,58,"2019-05-12"),(2,33,"2018-02-03"),(2,22,"2019-06-05"),(2,44,"2019-05-22")))
    val df = rowRDD.toDF("id","num","day")
    df.createOrReplaceTempView("sales")
    //按照id分组，按照销量num进行排序,排好序的前两条记录。
    //row_number() 函数实现
    spark.sql("SELECT id,num,day " +
      "FROM (" +
          "SELECT id" +
                ",num" +
                ",day" +
                ",row_number() over(partition by id order by num desc) as rank " +
          "FROM sales" +
          ") t1" +
      " WHERE rank<=2").show()
    //rank() 函数
    spark.sql("SELECT id" +
      ",num" +
      ",day" +
      ",rank() over(partition by id order by num desc) as rank " +
      "FROM sales").show()
    /*
        +---+---+----------+----+
        | id|num|       day|rank|
        +---+---+----------+----+
        |  1| 58|2019-05-12|   1|
        |  1| 45|2019-07-12|   2|
        |  1| 23|2019-06-21|   3|
        |  2| 44|2019-05-22|   1|
        |  2| 33|2018-02-03|   2|
        |  2| 22|2019-06-05|   3|
        +---+---+----------+----+
     */
  }
}
