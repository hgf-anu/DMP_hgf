package com.tags

import org.apache.spark.sql.Row

/**
  * 渠道标签
  */
object TagDitch extends Tag{
	override def makeTags(args:Any*):List[(String, Int)] ={
		var list = List[(String, Int)]()

		val row: Row = args(0).asInstanceOf[Row]

		val adplatformproviderid: String = row.getAs[String]("adplatformproviderid")

		list:+=("CN"+adplatformproviderid,1)

		list
	}
}
