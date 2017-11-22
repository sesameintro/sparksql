package com.zlcook.study

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Datasets相关操作,Dataset和Rdd类似，在结构化数据方面优势更好。
  * http://blog.csdn.net/sunbow0/article/details/50723233
  * Created by zhouliang6 on 2017/11/22.
  */
object DatasetsOperation  extends {

  val conf = new SparkConf().setAppName(" partition Discovery and Schema Merging").setMaster("local[*]").set("spark.driver.memory","2g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  //常见类转换成Ds
  val ds1 = Seq(1,2,3).toDS()
  ds1.map(f=>(f,f*2)).collect().foreach(println)

  //自定义类转换成Ds
  val ds2 =Seq(Person("zhouliang",20)).toDS()


  //DataFrame转换成Ds
  val path = "src/main/resources/people.json"
  val df = sqlContext.read.json(path)
  // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
  val ds3 = df.as[Person]



}
