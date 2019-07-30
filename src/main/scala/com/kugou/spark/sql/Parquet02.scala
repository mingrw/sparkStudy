package com.kugou.spark.sql

import org.apache.spark.sql.SparkSession

//测试parquet格式文件的分区推导  和  schema合并机制
object Parquet02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet")
      .master("local[*]")
      .getOrCreate()
    //创建测试数据
    import spark.implicits._
    //在parquet的一个key=1的目录存储以下数据，
    val df1 = spark.sparkContext.makeRDD(1 to 5).map(i=>(i,i*i)).toDF("value","square")
    df1.write.mode("overwrite").parquet("D:\\hadoop\\data\\parquet\\key=1")
    //在parquet的一个key=2的目录下存储以下数据
    val df2 = spark.sparkContext.makeRDD(6 to 10).map(i=>(i,i*i*i)).toDF("value","cube")
    df2.write.mode("overwrite").parquet("D:\\hadoop\\data\\parquet\\key=2")

    //以schema合并的方式读取parquet文件夹
    val df = spark.read.option("mergeSchema",true).parquet("D:\\hadoop\\data\\parquet")
    df.show()
    df.printSchema()
    df.createOrReplaceTempView("ming")
    spark.sql("select * from ming").show()
    /*
   root
 |-- value: integer (nullable = true)
 |-- square: integer (nullable = true)
 |-- cube: integer (nullable = true)
 |-- key: integer (nullable = true)        ---------------打印表结构时出现了key这一列(分区列)


 查询的结果：(不加option("mergeSchema" ,true))
 +-----+------+---+
|value|square|key|
+-----+------+---+
|    1|     1|  1|
|    2|     4|  1|
|    3|     9|  1|
|    4|    16|  1|
|    5|    25|  1|
|    6|  null|  2|
|    7|  null|  2|
|    8|  null|  2|
|    9|  null|  2|
|   10|  null|  2|
+-----+------+---+
加option("mergeSchema" ,true)
+-----+------+----+---+
|value|square|cube|key|
+-----+------+----+---+
|    1|     1|null|  1|
|    2|     4|null|  1|
|    3|     9|null|  1|
|    4|    16|null|  1|
|    5|    25|null|  1|
|    6|  null| 216|  2|
|    7|  null| 343|  2|
|    8|  null| 512|  2|
|    9|  null| 729|  2|
|   10|  null|1000|  2|
+-----+------+----+---+

     */
  }
}
