package com.zlcook.study

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 基本的DataFrame操作
  * Created by zhouliang6 on 2017/11/21.
  */

object BasicOperation extends  App{
  val conf = new SparkConf().setAppName("sparksql").setMaster("local[*]").set("spark.driver.memory","3g")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)
  val df = sqlContext.read.json("src/main/resources/people.json")

  df.registerTempTable("people")
  df.show

  val peoples = sqlContext.sql("SELECT name,age FROM people")
  peoples.map(p=>"name:"+p.getAs[String]("name")+",age:"+p.getAs("age")).collect().foreach(t=>println(t))

  val dftxt = sqlContext.read.text("src/main/resources/people.txt")
  dftxt.show()

  def log(msg:String): Unit ={
    println(msg)
  }
}

case class Person(name :String,age:Int)
