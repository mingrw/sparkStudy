package com.kugou.spark.core.exercise

import org.apache.spark.{SparkConf, SparkContext}

//自定义排序比较规则,
object Exercise05 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("E05").setMaster("local")
    val sc = new SparkContext(conf)
    val lineRdd = sc.parallelize(Array(("hadoop@apache",200), ("hive@apache",550), ("yarn@apache" ,580),("hive@apache",159),
      ("hadoop@apache",300), ("hive@apache",258), ("hadoop@apache",150), ("yarn@apache",560), ("yarn@apache",260)))
    //参数类型的省略：1.参数类型可以推出时，参数类型可以省略 2.只传入一个参数时,()可以省略 3.当参数只在=>右边出现一次时，可以用_替代参数
    val resultRdd = lineRdd.map(line=>{
      (new SortBean(line._1,line._2),1)//显然只能满足到2，因为参数在=>右边出现了两次
    }).sortByKey().map(line=>{
      line._1.str+" "+line._1.score
    }).foreach(println)

  }
}

//自定义排序比较规则需要重写Comparable中的compareTo方法或者Ordered中的compare方法，同时继承Serializable
class SortBean(str_in:String,score_in:Int) extends Ordered[SortBean] with Serializable {
  val str = str_in
  val score = score_in
  //先按字符串内容进行比较，内容相同按照整数值大小进行比较
  override def compare(o: SortBean): Int = {
    if(this.str > o.str) 1
    else if(this.str<o.str) -1
    else {
      if(this.score==o.score) 0
      if(this.score>o.score) 1
      else -1
    }
  }
}
