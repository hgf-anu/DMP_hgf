package com.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object TagLocation extends Tag{
	override def makeTags(args:Any*):List[(String, Int)] ={
		var list = List[(String, Int)]()

		val row: Row = args(0).asInstanceOf[Row]

		//获取省市
		val pro: String = row.getAs[String]("provincename")
		val city: String = row.getAs[String]("cityname")

		if(StringUtils.isNotBlank(pro)){
			list:+=("ZP"+pro,1)
		}

		if(StringUtils.isNotBlank(city)){
			list:+=("ZC"+city,1)
		}

		list
	}
}
