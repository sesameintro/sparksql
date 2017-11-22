package com.zlcook.study

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将Rdd转换成DataFrame
  * Created by zhouliang6 on 2017/11/22.
  */
object InteropWithRdd extends App{

  val conf = new SparkConf().setAppName("Interoprating with RDDs").setMaster("local[*]").set("spark.driver.memory","2g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // first method : programmatically specifying the schema
  val personRdd =sc.textFile("src/main/resources/people.txt")
  val columns = "name,age"
  val schema = StructType(columns.split(",").map(c => StructField(c,StringType,true)))
  // convert records of RDD (people) to Rows
  val personRowRdd = personRdd.map(_.split(",")).map(p => Row(p(0),p(1).trim ) )
  // Apply the schema to the RDD
  val personDf = sqlContext.createDataFrame(personRowRdd, schema)

  personDf.registerTempTable("person")
  val results = sqlContext.sql("select name from person")
  results.map( r =>"name:" +r(0)).collect().foreach( println )

  // second method: interring the schema using Reflection

  // This is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._
  val peopleDf = sc.textFile("src/main/resources/people.txt").map(_.split(",")).map( p => Person(p(0),p(1).trim.toInt) ).toDF()
  peopleDf.registerTempTable("people")

  peopleDf.write.format("parquet").mode(SaveMode.Overwrite).save("src/main/resources/people.parquet")

  val parquetFile = sqlContext.read.parquet("src/main/resources/people.parquet")
  parquetFile.registerTempTable("parquetFile")
  val teen = sqlContext.sql("select name from parquetFile where age > 13")
  teen.map( t => "Fname:"+t(0)).collect().foreach(println)

}
