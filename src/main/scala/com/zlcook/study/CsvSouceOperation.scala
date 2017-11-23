package com.zlcook.study

import java.io.File

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.types.{ShortType, StringType, StructField, StructType}

import scala.io.Source

/**
  * 从csv中读取数据，1.保存到json文件。2.定制化json格式保存到文件
  * Created by zhouliang6 on 2017/11/23.
  */
object CsvSouceOperation extends App{

  val conf = new SparkConf().setAppName("csv operation").setMaster("local[3]").set("spark.driver.memory","2g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


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

  //json格式保存到文件中
  df.repartition(32, df("name"))
  df.coalesce(1).write.format("json").mode(SaveMode.Overwrite).save("src/main/resources/peoplecsv2json")


  val file = new File("src/main/resources/peoplecsv2customJson")
  deleteDir(file)
  //定制输出格式，并保存到json文件中
  df.map( Animal(_,"people") ).saveAsTextFile("src/main/resources/peoplecsv2customJson")

  def deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
        println("delete file " + f.getAbsolutePath)
      }
    })
    dir.delete()
    println("delete dir " + dir.getAbsolutePath)
  }

}
