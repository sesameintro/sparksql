package com.zlcook.study

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by zhouliang6 on 2017/11/22.
  */
object HiveTables extends App{
  val conf = new SparkConf().setAppName("hive table").setMaster("local[*]").set("spark.driver.memory","2g")
  val sc = new SparkContext(conf)


}
