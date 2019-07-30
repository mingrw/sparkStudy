package com.kugou.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}



//对spark SQL的一些操作进行实践,DSL风格
object SparkSQLAction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("parquet")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val df = spark.sparkContext.parallelize(Array(("marry",20),("kitty",19),("tom",22),("jack",23),("jane",18)),2).toDF("name","age")

    //show操作
    df.show(2)  //显示前两条
    df.show(false)  //　是否最多只显示20个字符，默认为true。

    //printSchema
    df.printSchema()    //打印df的schema信息

    //获取所有数据到数组 Array[Row]类型
    df.collect()

    //顾名思义  List[Row]类型
    df.collectAsList()

    //获取指定字段的统计信息(可同时统计多个列)  统计一下指标：count(记录数), mean（均值）, stddev（方差）, min（最小值）, max（最大值）
    df.describe("age")

    //first, head, take, takeAsList：获取若干行记录
    df.first()     //返回第一行记录，Row类型
    df.head(2)     //返回头两条记录，Array[Row]类型
    df.take(2)    //返回头两条记录，Array[Row]类型   -----head和first作用一致    head(n)和take(n)作用一致
    df.takeAsList(2) //顾名思义

    //where 和 filter操作
    df.where($"age">20).show() //显示age>20d的记录  说明：$和列名配合使用指的返回列对象
    df.where("age>20").show() //和以上效果相同
    df.filter(row=>row.getInt(1)>20).show()   //filter支持传入函数 ，同时filter也支持where的操作形式(建议用filter取代where)

    //查询指定字段 select selectExpr drop
    df.select($"name",$"age").show()
    df.select("name","age").show()  //效果相同，显示name和age列
    df.selectExpr("name","age as old").show() //对指定字段进行处理(也可调用UDF函数处理)，此处时给age列起了个别名
    df.col("name")   //获取指定列名的列对象 返回类型Column (效果等同于$"name")
    df.apply("name")     //获取指定列名的列对象 返回类型Column (效果等同于$"name")
    df.drop("name").show()     //删除指定字段

    //limit操作
    df.limit(3).show()    //取前三条记录 ，limit和take的区别在于take操作为action操作，limit不是，需要action操作触发执行

    //order by 和sort以及sortWithinPartitions
    df.orderBy($"age".desc).show() //按照age降序,默认升序
    df.orderBy("age").show()    //按照age排序,升序
    df.sort($"name".desc)
    df.sort("age")       //sort用法和orderBy完全相同
    df.sortWithinPartitions("age").show()   //按照分区排好序，不进行汇总排序

    //准备些数据用于进行分组聚合操作
    val df1 = spark.sparkContext.parallelize(Array((1,"chinese",80),(1,"math",90),(2,"chinese",98),(2,"math",99),(3,"math",100))).toDF("sno","course","grade")

    //group by操作,可以和聚合函数一起使用,使用效果同SQL
    df1.groupBy("sno").max("grade").show()  //按照sno分组，求每组grade最大的信息
    df1.groupBy("sno").min("grade").show()
    df1.groupBy("sno").avg("grade").show()
    df1.groupBy("sno").sum("grade").show()  //顾名思义
    df1.groupBy("sno").count()               //统计组内记录记录数

    //agg操作，聚合，与group 操作一起使用，agg操作可以同时执行多种聚合操作(以map方式传入 列名->操作名)
    df1.groupBy("sno").agg("grade"->"max","grade"->"sum").show()  //求每组成绩的最大值和累加和
    df1.groupBy("sno").agg(("grade","max"),("grade","sum")).show()  //同上，表达Map表现形式不同
//    df1.groupBy("sno").agg($"name").show()   传入Column类型参数如何使用？？？

    //union操作  类比理解MySQL中的unionAll ，将两个列数相同的DataFrame进行合并
    val df2 = spark.sparkContext.parallelize(Array(("marry",20),("gao",18))).toDF("sname","age")
    df.union(df2).show()  //df合并df2,即使字段名和字段类型不一致也能进行合并。

    //distinct操作  去重
    df1.distinct().show()     //返回不重复的Row记录集合
    df1.dropDuplicates("sno").show()  //按照指定列进行去重

    //join操作
    //先造两个DataFrame用于join测试
    val joinDF1 = spark.sparkContext.parallelize(Array((1,"marry"),(2,"tom"),(3,"kitty"),(4,"jack"))).toDF("sno","sname")
    val joinDF2 = spark.sparkContext.parallelize(Array((1,"chinese",80),(1,"math",90),(2,"chinese",98),(2,"math",99),(3,"math",100),(5,"math",100))).toDF("sno","course","grade")
    joinDF1.join(joinDF2,"sno").show()   //inner join ,此处实际上省略了joinType，默认是inner 还有outer(full),left_outer(left),right_outer（right）
    //joinDF1.join(joinDF2,joinDF1("sno"),"inner").show()  //和上一句功能完全一致,区别在与此种方式可以选择joinType
    joinDF1.join(joinDF2,Array("sno"),"inner").show()   //和上一句完全相同，区别在于可以选择多个列作为join条件
    joinDF1.join(joinDF2,Array("sno"),"left_outer").show()   //左外连接
    joinDF1.join(joinDF2,Array("sno"),"right_outer").show()  //右外连接
    joinDF1.join(joinDF2,Array("sno"),"outer").show()      //全外连接
    joinDF1.join(joinDF1,Array("sno"),"leftsemi").show()    //left semi join是以左表为准，在右表中查找匹配的记录，
                                                                        // 如果查找成功，则仅返回左边的记录，否则返回null
    joinDF1.join(joinDF2,Array("sno"),"leftanti").show()    //left anti join与left semi join相反，是以左表为准，在右表中查找匹配的记录，
                                                                        // 如果查找成功，则返回null，否则仅返回左边的记录
    joinDF1.crossJoin(joinDF2).show()     //笛卡尔积

    //intersect方法可以计算出两个DataFrame中相同的记录
    df.intersect(df2).show()

    //expect操作可以计算出一个DataFrame对比于另一个独有的记录
    df.except(df2).show()              //思考MySQL如何实现

    //withColumn和withColumnRename
    df.withColumnRenamed("name","sname")  //修改已存在的列名
   // df.withColumn("sex",df1("sex"))             //增加一列,如何操作column对象来增加一列？？？？
    df.printSchema()

    //行转列的explode函数：可以将一列数据进行拆分成多列,已废弃，可以使用flatMap替代
    //构造一个df用于测试
    val explode_df = spark.sparkContext
      .parallelize(Array(("1","2019-07-23 09:22:33"),("2","2019-07-22 06:22:58"),("3","2019-05-23 09:12:23")))
      .toDF("id","time")
    explode_df.show()
    explode_df.explode("time","dayAndHour"){time:String=>time.split(" ")}.show()//将一行time拆分成两行
    explode_df.flatMap(_.getString(1).split(" ")).show()   //将一行time拆分成两行
    //以下为更好的实现版本
    val explode_rdd = explode_df.rdd.map(row=>Row(row.getString(0),row.getString(1).split(" ")(0),row.getString(1).split(" ")(1)))
    val schema = StructType(Array(StructField("id",StringType,false),StructField("day",StringType,false),StructField("hour",StringType,false)))
    spark.createDataFrame(explode_rdd,schema).show()
    spark.stop()
  }
}
