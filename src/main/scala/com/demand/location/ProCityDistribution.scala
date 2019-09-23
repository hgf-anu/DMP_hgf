package com.demand.location

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ProCityDistribution{
	def main(args:Array[String]):Unit ={

		if( args.length != 1 ) {
			println( "目录不正确，退出程序" )
			sys.exit()
		}

		val Array( inputPath ) = args

		val conf:SparkConf = new SparkConf().setAppName( this.getClass.getName ).setMaster( "local[*]" ).set(
			"spark.serializer",
			"org.apache.spark.serializer.KryoSerializer" ).set( "spark.io.compression.codec", "Snappy" )

		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
		val df: DataFrame = spark.read.parquet(inputPath)
		df.createOrReplaceTempView( "log" )

		// requestmode 数据请求方式（1:请求、2:展示、3:点击）
		// processnode 流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
		// iseffective 有效标识（有效指可以正常计费的）(0：无效 1：有效
		// isbilling 是否收费（0：未收费 1：已收费）
		// isbid 是否rtb
		// iswin 是否竞价成功
		// adordeerid
		//winprice rtb竞价成功价格
		//adpayment 转换后的广告消费
		/*
		val df2:DataFrame = spark.sql(
			"""
			select
            sessionid,
			 provincename,
			 cityname,
			 case when requestmode=1 and processnode>=1 then 1  end rawRequest,
			 case when requestmode=1 and processnode>=2 then 1  end effectiveRequest,
			 case when requestmode=1 and processnode=3 then 1  end AdRequest,
			 case when iseffective=1 and isbilling=1 and isbid=1 then 1 end bidCnt,
			 case when iseffective=1 and isbilling=1 and iswin=1 and adorderid<>0 then 1 end bidWinCnt,
			 case when requestmode=2 and iseffective=1 then 1 end showCnt,
			 case when requestmode=3 and iseffective=1 then 1 end clickCnt,
			 case when iseffective=1 and isbilling=1 and iswin=1  then winprice/1000 end AdCost,
			 case when iseffective=1 and isbilling=1 and iswin=1  then adpayment/1000 end AdPayment
			from log
			""" )
		*/
		val df2:DataFrame = spark.sql(
			"""
	        select
				 provincename,
				 cityname,
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
					provincename,
					cityname,
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
			group by provincename,cityname

			""" )
		df2.show(1000)
		//df2.write.partitionBy("provincename","cityname").parquet("E:\\千锋\\大数据-第三阶段-Spark项目\\项目代码\\outputData\\ProCityDistribution")
		spark.stop()
	}
}
