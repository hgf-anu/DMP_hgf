package com.tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * APP名称标签
  */
object TagAppname extends Tag{
	override def makeTags(args:Any*):List[(String, Int)] ={

		var list:List[(String,Int)] = List[(String,Int)]()
		//1.1接受row,类型下转为Row
		val row: Row = args(0).asInstanceOf[Row]

		//1.2通过参数收到广播变量
		val app_dict: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

		//1.3获取appname和appid
		val appname: String = row.getAs[String]("appname")
		val appid: String = row.getAs[String]("appid")

		//1.4带上前缀APP,如果为空就使用'其他'来标记
		if(StringUtils.isNotBlank(appname)){
			list:+=("APP"+appname,1)
		}else{
			//广播变量的value返回广播的数据,通过appid关联
			//这里的0和1代表什么?
			list:+=("APP"+app_dict.value.getOrElse(appid,"其他"),1)
		}
		list
	}
}
