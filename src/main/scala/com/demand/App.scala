package com.demand

import com.util.RptUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


class App{

}

/**
  * 通过sparkcore和RptUtil联合统计
  */
object App{
	def main(args:Array[String]):Unit ={
		if( args.length != 3 ) {
			println( "输入目录不正确" )
			sys.exit()
		}

		val Array( inputPath, outputPath, docs ) = args
		val conf:SparkConf = new SparkConf().setAppName( this.getClass.getName ).setMaster( "local[*]" ).set(
			"spark.serializer",
			"org.apache.spark.serializer.KryoSerializer" ).set( "spark.io.compression.codec", "Snappy" )

		val spark:SparkSession = SparkSession.builder().config( conf ).getOrCreate()

		val sc:SparkContext = spark.sparkContext
		//1.读取数据字典
		val docMap:collection.Map[String, String] = sc.textFile( docs ).map( _.split( "\\s",
		                                                                              -1 ) ).filter( _.length >= 5 ).map(
			arr => {
				(arr( 4 ), arr( 1 ))
			} ).collectAsMap()
		//2.进行广播
		val broadcast = sc.broadcast( docMap )

		//3.读取数据文件
		val df:DataFrame = spark.read.parquet( inputPath )
		val appRDD:RDD[(String, List[Double])] = df.rdd.map( row => {
			//4.取媒体相关字段
			val appName:String = row.getAs[String]( "appname" )
			if( StringUtils.isBlank( appName ) ) {
				val appName:String = broadcast.value.getOrElse( row.getAs[String]( "appid" ), "unknown" )
			}
			val requestmode = row.getAs[Int]( "requestmode" )
			val processnode = row.getAs[Int]( "processnode" )
			val iseffective = row.getAs[Int]( "iseffective" )
			val isbilling = row.getAs[Int]( "isbilling" )
			val isbid = row.getAs[Int]( "isbid" )
			val iswin = row.getAs[Int]( "iswin" )
			val adordeerid = row.getAs[Int]( "adorderid" )
			val winprice = row.getAs[Double]( "winprice" )
			val adpayment = row.getAs[Double]( "adpayment" )

			//4.1处理请求数
			val rptList:List[Double] = RptUtil.repPt( requestmode, processnode )
			//4.2处理展示点击
			val clickList:List[Double] = RptUtil.clickPt( requestmode, iseffective )
			//4.3处理广告
			val adList:List[Double] = RptUtil.adPt( iseffective,
			                                        isbilling,
			                                        isbid,
			                                        iswin,
			                                        adordeerid,
			                                        winprice,
			                                        adpayment )

			//4.4所有指标
			val allList:List[Double] = rptList ++ clickList ++ adList
			(appName, allList)
		} )
		val aggRDD:RDD[(String, List[Double])] = appRDD.reduceByKey( (list1, list2) => {
			list1.zip( list2 ).map( t => t._1 + t._2 )
		} )
		val showRDD:RDD[String] = aggRDD.map( t => {
			t._1 + ":" + t._2.mkString( "," )
		} )
		showRDD.collect().foreach(println)
	}
}
