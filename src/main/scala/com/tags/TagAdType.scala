package com.tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告位类型标签
  * App 名称（标签格式： APPxxxx->1）xxxx 为 App 名称，使用缓存文件 appname_dict 进行名称转换；APP 爱奇艺->1
  */
object TagAdType extends Tag{
	override def makeTags(args:Any*):List[(String, Int)] ={
		var list: List[(String, Int)] = List[(String,Int)]()
		//1.获取数据类型
		val row: Row = args(0).asInstanceOf[Row]
		//2.获取广告位类型和名称
		val adType: Int = row.getAs[Int]("adspacetype")
		//3.广告位类型标签
		adType match{
			case v if v>9 => list:+=("LC"+v,1)
			case v if v<=9 => list:+=("LC0"+v,1)
		}
		//4.广告名称
		val adName: String = row.getAs[String]("adspacetypename")
		if(StringUtils.isNotBlank(adName)){
			list:+=("LN"+adName,1)
		}
		list
	}
}
