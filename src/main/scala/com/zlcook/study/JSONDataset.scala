package com.zlcook.study

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by zhouliang6 on 2017/11/22.
  */
object JSONDataset extends  App{
  val conf = new SparkConf().setAppName(" partition Discovery and Schema Merging").setMaster("local[*]").set("spark.driver.memory","2g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // A JSON dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files.
  val path = "src/main/resources/people.json"
  val people = sqlContext.read.json(path)

  // The inferred schema can be visualized using the printSchema() method.
  people.printSchema()
  // root
  //  |-- age: integer (nullable = true)
  //  |-- name: string (nullable = true)

  // Register this DataFrame as a table.
  people.registerTempTable("people")

  // SQL statements can be run by using the sql methods provided by sqlContext.
  val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

  // Alternatively, a DataFrame can be created for a JSON dataset represented by
  // an RDD[String] storing one JSON object per string.
  val anotherPeopleRDD = sc.parallelize(
    """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
  val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
  anotherPeople.printSchema()
  /*
  root
  |-- address: struct (nullable = true)
  |    |-- city: string (nullable = true)
  |    |-- state: string (nullable = true)
  |-- name: string (nullable = true)
  */
}
