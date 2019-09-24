package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConnectionPool, String2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * (重点)商圈标签,联和
  * HttpUtil,通过http协议发送消息到高德
  * JedisConnectionPool,中间消息存储到redis
  * AmapUtil,通过第三方工具,获取商圈信息
  */
object TagBusiness extends Tag{

	//样例示范:
	// 1.在浏览器输入:https://restapi.amap.com/v3/geocode/regeo?output=json&location=116.310003,39.991957&key=1dfc8e421420d54eaa1065dc6a07ef17&radius=1000&extensions=all
	//2.在代码中通过HTTP/HTTPS协议发送请求
	override def makeTags(args:Any*):List[(String, Int)] ={

		var list: List[(String, Int)] = List[(String, Int)]()

		val row: Row = args(0).asInstanceOf[Row]

		// 1.获取经纬度
		val longitude: Double = String2Type.toDouble(row.getAs[String]("long"))
		val latitude:Double = String2Type.toDouble( row.getAs[String]( "lat" ) )

		// 2.过滤条件:
		// 中国经度(longitude)范围：73°33′E至135°05′E；
		// 纬度(latitude)范围：3°51′N至53°33′N
		// *注意:高德地图经纬度传递的顺序是:经度,纬度.
		// 常见查询经纬度网站的顺序是:纬度,经度[纬度在前，中间逗号，纬度范围-90~90，经度范围-180~180]
		// 注意数据格式是否满足:经度,纬度.如果是纬度,经度.则需要改动代码

		if(longitude>=73 && longitude <=136 &&latitude>=3 && latitude<=54){
			// 获取到商圈名称
			val business = getBusiness(longitude,latitude)
			if(StringUtils.isNotBlank(business)){
				val str = business.split(",")
				str.foreach(str=>{
					list:+=(str,1)
				})
			}
		}
		list
	}

	/**
	*   使用AmapUtil类的方法获得商圈信息
	* @param long
	* @param lat
	* @return business
	  */
	def getBusiness(long:Double, lat:Double) :String={
		//1.先去redis中查找
		//GeoHash码,这里的函数传递的参数是维度,经度,精确程度
		val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)

		// 数据库查询当前商圈信息
		var business: String = redis_queryBusiness(geohash)

		//2.如果返回值是空值的话就去高德网站上请求
		if(StringUtils.isBlank(business)){
			business = AmapUtil.getBusinessFromAmap( long, lat )
			//将新获得的商圈信息缓存到redis中
			if(StringUtils.isNotBlank(business)){
				redis_insertBusiness(geohash,business)
			}
		}
		business
	}
	/**
	  * 数据库获取商圈信息,构建redis连接池
	  * @param geohash
	  * @return
	  */
	def redis_queryBusiness(geohash:String):String={
		val jedis = JedisConnectionPool.getConnection()
		val business = jedis.get(geohash)
		jedis.close()
		business
	}

	/**
	  * 每次从json串中解析出商圈中,就将商圈保存在redis数据库中,每一次解析前先到redis中遍历一遍,节省大量去高德网站上的请求时间
	  */
	def redis_insertBusiness(geohash:String,business:String): Unit ={
		val jedis = JedisConnectionPool.getConnection()
		jedis.set(geohash,business)
		jedis.close()
	}
}
