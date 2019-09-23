package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * Http请求协议
  */
object HttpUtil{
	/**
	  * GET请求
	  * @param url
	  * @return Json,格式是String
	  */
	def get(url:String):String={
		val client: CloseableHttpClient = HttpClients.createDefault()
		val httpGet = new HttpGet(url)
		//1.发送请求
		val httpResponse: CloseableHttpResponse = client.execute(httpGet)
		//2.处理返回请求结果,防止乱码现象
		EntityUtils.toString(httpResponse.getEntity,"UTF-8")
	}
}
