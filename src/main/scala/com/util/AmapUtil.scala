package com.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 从高德地图获取商圈信息
  */
object AmapUtil{
	//https://restapi.amap.com/v3/geocode/regeo?output=json&location=116.310003,39.991957&key=1dfc8e421420d54eaa1065dc6a07ef17&radius=1000&extensions=all
	/**
	  * 解析经纬度
	  * @param long
	  * @param lat
	  * @return
	  */
	def getBusinessFromAmap(long:Double,lat:Double):String={
		val location: String = long+","+lat
		//1.获取URL
		val url:String = "https://restapi.amap.com/v3/geocode/regeo?output=json&location=" + location + "&key=1dfc8e421420d54eaa1065dc6a07ef17&radius=1000&extensions=all"
		//2.调用Http接口发送请求
		val jsonStr: String = HttpUtil.get(url)
		//3.解析JSON串
		val jSONObject1: JSONObject = JSON.parseObject(jsonStr)
		//3.1判断当前状态(status)是否为1
	  	val status: Int = jSONObject1.getIntValue("status")
		if(status==0) return ""

		//3.2如果status不为空,解析下一层到regeocode
		val jSONObject2: JSONObject = jSONObject1.getJSONObject("regeocode")
		if(jSONObject2==null) return ""

		//3.3解析下一层到addressComponent
		val jSONObject3: JSONObject = jSONObject2.getJSONObject("addressComponent")
		if(jSONObject3==null) return ""

		//3.4解析到下一层businessAreas[商圈],是数组形式,存储一个范围的数据
		val jSONArray: JSONArray = jSONObject3.getJSONArray("businessAreas")
		if(jSONArray==null) return ""

		//4.
		// 4.1定义集合取值
		val result:ListBuffer[String] = mutable.ListBuffer[String]()
		//4.2循环数组,取出名字(name),添加到可变化列表
		for(item<-jSONArray.toArray()){
			if(item.isInstanceOf[JSONObject]){
				val json:JSONObject = item.asInstanceOf[JSONObject]
				val name:String = json.getString( "name" )
				result.append(name)
			}
		}
		//4.3.商圈名字
		result.mkString(",")
	}
}
