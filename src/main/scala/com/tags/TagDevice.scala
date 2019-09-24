package com.tags

import org.apache.spark.sql.Row

/**
  * 设备标签
  */
object TagDevice extends Tag{
	override def makeTags(args:Any*):List[(String, Int)] ={
		var list:List[(String, Int)] = List[(String, Int)]()

		val row: Row = args(0).asInstanceOf[Row]

		//1.1获取操作系统
		val client: Int = row.getAs[Int]("client")

		//1.2获取联网方式
		val network: String = row.getAs[String]("networkmannername")

		//1.3获取运营商
		val ispName: String = row.getAs[String]("ispname")

		//2.1操作系统 (1)Android D00010001 (2)IOS D00010002 (3)WinPhone D00010003 (4)_ 其 他 D00010004
		client match {
			case 1 => list:+=("D00010001",1)
			case 2 => list:+=("D00010002",1)
			case 3 => list:+=("D00010003",1)
			case _ => list:+=("D00010004",1)
		}

		//2.2设备联网方式 (1)WIFI D00020001 (2)4G D00020002 (3)3G D00020003 (4)2G D00020004 (5)_ D00020005
		network match {
			case "Wifi" => list:+=("D00020001 ",1)
			case "4G" => list:+=("D00020002 ",1)
			case "3G" => list:+=("D00020003 ",1)
			case "2G" => list:+=("D00020004 ",1)
			case _ => list:+=("D00020005 ",1)
		}

		//2.3设备运营商 (1)移 动 D00030001 (2)联 通 D00030002 (3)电 信 D00030003 (4)_ D00030004
		ispName match {
			case "移动" => list:+=("D00030001",1)
			case "联通" => list:+=("D00030002",1)
			case "电信" => list:+=("D00030003",1)
			case _ => list:+=("D00030004",1)
		}

		list
	}
}
