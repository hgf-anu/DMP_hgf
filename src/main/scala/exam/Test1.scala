package exam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Test1{
	def main(args:Array[String]):Unit ={
		val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
		val sc = new SparkContext(conf)
		val sQLContext = new SQLContext(sc)

		val jsonStr: RDD[String] = sc.textFile("File/json.txt")

		val logs: mutable.Buffer[String] = jsonStr.collect().toBuffer

		var list:List[List[String]] = List()

		for(i <- logs.indices){
			val logStr: String = logs(i).toString

			val jSONObject1 = JSON.parseObject(logStr)
			// 判断当前状态是否为 1
			val status = jSONObject1.getIntValue("status")
			if(status == 0) return ""
			// 如果不为空
			val jSONObject = jSONObject1.getJSONObject("regeocode")
			if(jSONObject == null) return ""

			val jSONArray = jSONObject.getJSONArray("pois")
			if(jSONArray == null) return ""

			// 定义集合取值
			val result = collection.mutable.ListBuffer[String]()

			// 循环数组
			for (item <- jSONArray.toArray()){
				if(item.isInstanceOf[JSONObject]){
					val json = item.asInstanceOf[JSONObject]
					val businessarea = json.getString("businessarea")
					result.append(businessarea)
				}
				val tempList: List[String] = result.toList
				list:+=tempList
			}

			val allList: List[String] = list.flatMap(list1=>list1)
			val tuples:List[(String, Int)] = allList.map( l => {
				val listStr:String = l.substring( 1, l.length - 1 )
				(listStr, 1)
			} )
			val grouped: Map[String, List[(String, Int)]] = tuples.groupBy(x=>x._1)
			val sorted: List[(String, Int)] = grouped.mapValues(x=>x.size).toList.sortBy(x=>x._2)

			sorted.foreach(println)
		}

	}

}
