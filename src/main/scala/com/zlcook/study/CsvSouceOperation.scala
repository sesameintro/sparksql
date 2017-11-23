package com.zlcook.study

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ShortType, StringType, StructField, StructType}

/**
  * Created by zhouliang6 on 2017/11/23.
  */
object CsvSouceOperation extends App{

  val conf = new SparkConf().setAppName("csv operation").setMaster("local[3]").set("spark.driver.memory","2g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val path ="src/main/resources/people.csv"

  val schema = StructType(
    Array(
      StructField("name",StringType,true),
      StructField("age",ShortType,false)
    )
  )
  val options = Map("header"->"true","path"->path,"delimiter" -> "," )
  val df = sqlContext.read.format("com.databricks.spark.csv").options(options).schema(schema).load()
  df.printSchema()

  df.show()




}
