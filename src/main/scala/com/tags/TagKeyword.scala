package com.tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  * 关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能超过 8 个字符；关键字中如包含"|"，则分割成数组，转化成多个关键字标签
  *
  */
object TagKeyword extends Tag{
	override def makeTags(args:Any*):List[(String, Int)] ={
		var list:List[(String, Int)] = List[(String, Int)]()

		val row:Row = args( 0 ).asInstanceOf[Row]

		val keywords:String = row.getAs[String]( "keywords" )

		//接受黑名单:广播变量
		val banWords:Broadcast[collection.Map[String, Int]] = args( 1 ).asInstanceOf[Broadcast[collection.Map[String, Int]]]

		//注意加上转义
		val kwords:Array[String] = keywords.split( "\\|" )

		//过滤条件
		for( word <- kwords ) {
			if( word.length >= 3 && word.length <= 8 && ( !banWords.value.contains( word ) ) ) {
				list :+= ("K" + word, 1)
			}
		}

		list
	}
}
