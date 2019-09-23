package com.tags

/**
  * (重点)商圈标签,联和
  * HttpUtil,通过http协议发送消息到高德
  * JedisConnectionPool,中间消息存储到redis
  * AmapUtil,通过第三方工具,获取商圈信息
  */
object TagBusiness extends Tag{
//https://restapi.amap.com/v3/geocode/regeo?output=json&location=116.310003,39.991957&key=1dfc8e421420d54eaa1065dc6a07ef17&radius=1000&extensions=all
	override def makeTags(args:Any*):List[(String, Int)] ={

	}
}
