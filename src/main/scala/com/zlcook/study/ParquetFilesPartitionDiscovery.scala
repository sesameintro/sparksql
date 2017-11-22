package com.zlcook.study

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Partition Discovery and Schema Merging
  * Created by zhouliang6 on 2017/11/22.
  */
object ParquetFilesPartitionDiscovery extends App{

  val conf = new SparkConf().setAppName(" partition Discovery and Schema Merging").setMaster("local[*]").set("spark.driver.memory","2g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  //This is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._

  val df1 = sc.makeRDD( 1 to 5).map( f => ( f, f*2)).toDF("single","double")
  // the method below is equal to df1.write.mode(SaveMode.Overwrite).parquet("src/main/resources/test_table/key=1")
  df1.write.mode(SaveMode.Overwrite).format("parquet").save("src/main/resources/test_table/key=1")

  val df2 = sc.makeRDD(6 to 10).map( i=> (i, i * 2)).toDF("single","triple")
  df2.write.mode(SaveMode.Overwrite).parquet("src/main/resources/test_table/key=2")

  //sqlContext.read.option("mergeSchema","true").format("parquet").load("src/main/resources/test_table")
  val df = sqlContext.read.option("mergeSchema","true").parquet("src/main/resources/test_table")
  df.printSchema()

  df.registerTempTable("parquetFile")
  val da = sqlContext.sql("select single from parquetFile where double > 2 and double < 9")
  da.map(r => "sigle:"+r(0)).collect().foreach(println(_))
}
