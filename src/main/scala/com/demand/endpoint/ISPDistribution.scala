package com.demand.endpoint

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 运营商分布
  */

object ISPDistribution{
	def main(args:Array[String]):Unit ={

		if( args.length != 1 ) {
			println( "目录不正确，退出程序" )
			sys.exit()
		}

		val Array( inputPath ) = args

		val conf:SparkConf = new SparkConf().setAppName( this.getClass.getName ).setMaster( "local[*]" ).set(
			"spark.serializer",
			"org.apache.spark.serializer.KryoSerializer" ).set( "spark.io.compression.codec", "Snappy" )

		val spark:SparkSession = SparkSession.builder().config( conf ).getOrCreate()
		val df:DataFrame = spark.read.parquet( inputPath )
		df.createOrReplaceTempView( "log" )

		val df2:DataFrame = spark.sql(
			"""
	        select
				 ispname,
                sum(rawRequest) rawRequest,
                sum(effectiveRequest) effectiveRequest,
                sum(AdRequest) AdRequest,
                sum(bidCnt) bidCnt,
                sum(bidWinCnt) bidWinCnt,
                sum(showCnt) showCnt,
                sum(clickCnt) clickCnt,
                sum(AdCost) AdCost,
                sum(AdPayment) AdPayment
		    from(
				select
					sessionid,
					ispname,
					case when requestmode=1 and processnode>=1 then 1  end rawRequest,
					case when requestmode=1 and processnode>=2 then 1  end effectiveRequest,
					case when requestmode=1 and processnode=3 then 1  end AdRequest,
					case when iseffective=1 and isbilling=1 and isbid=1 then 1 end bidCnt,
					case when iseffective=1 and isbilling=1 and iswin=1 and adorderid<>0 then 1 end bidWinCnt,
					case when requestmode=2 and iseffective=1 then 1 end showCnt,
					case when requestmode=3 and iseffective=1 then 1 end clickCnt,
					case when iseffective=1 and isbilling=1 and iswin=1  then winprice/1000 end AdCost,
					case when iseffective=1 and isbilling=1 and iswin=1  then adpayment/1000 end AdPayment
				from log) t
			group by ispname

			""" )
		df2.show( 1000 )
	}
}
