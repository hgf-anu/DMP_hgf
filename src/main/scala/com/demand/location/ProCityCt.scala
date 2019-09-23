package com.demand.location
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
* 统计省市指标
* */
object ProCityCt{
	def main(args:Array[String]):Unit ={
		if(args.length!=1){
			println("输入目录不正确")
			sys.exit()
		}
		val Array(inputPath) = args
		val spark = SparkSession.builder().appName("ct").master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

		//获取数据
		val df: DataFrame = spark.read.parquet(inputPath)
		//注册临时视图
		df.createOrReplaceTempView("log")
		val df2: DataFrame = spark.sql(
			"""
			select
			 provincename,
			 cityname,
			 count(*) ct
			from log
			group by provincename,cityname
			""")
		//需求1:按照省市进行分区存储
		df2.write.partitionBy("provincename","cityname").json("E:\\千锋\\大数据-第三阶段-Spark项目\\项目代码\\outputData\\procity")

		//需求2:存储在MySQL,通过配置文件进行加载
		//通过config配置文件依赖进行加载相关的配置信息
		/*
		val load: Config = ConfigFactory.load()
		//创建properties对象
		val prop = new Properties()
		prop.setProperty("user",load.getString("jdbc.user"))
		prop.setProperty("password",load.getString("jdbc.password"))
		//存储
		df2.write.mode( SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),prop)
		*/

		spark.stop()
	}

}
