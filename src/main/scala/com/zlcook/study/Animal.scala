package com.zlcook.study

import java.util.UUID

import org.apache.spark.sql.Row

/**
  * Created by zhouliang6 on 2017/11/23.
  */
class Animal(uuidGenerator:()=> UUID, properties:Array[(String,Any)], label:String="animal") {

  val id = uuidGenerator()

  private[this] val propertyArray = properties.map { f =>
    f match {
      case (p, null) =>
        s""""${label + "_" + p}":{"id":"${uuidGenerator()}","value":"20"}"""
      case (p, v) => s""" "${label + "_" + p}":{"id":"${uuidGenerator()}","value":"$v"}"""
    }
  }
  override def toString = {
    val projson = propertyArray.mkString(",")
      s"""{"id":"$id","label":"$label","properties":{$projson}}"""
  }
}

object Animal {

  val properyName=Array(
    "name",
    "age"
  )

  def apply(uuidGenerator: () => UUID, properties: Array[(String, Any)], label: String): Animal = new Animal(uuidGenerator, properties, label)

  def apply(row:Row,label:String):Animal = new Animal( ()=>UUID.randomUUID(),properyName.map{
    case p => {
      val index = row.fieldIndex(p)
      val v =row.get(index)
      if(v == null)
        (p,null)
      else if( v.isInstanceOf[String]){
        (p,v.asInstanceOf[String])
      }else
        (p,v)
    }
  },label)
}