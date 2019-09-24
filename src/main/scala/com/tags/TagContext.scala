package com.tags

import com.typesafe.config.ConfigFactory
import com.util.TagUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 上下文标签
  * 入口程序,连接所有的标签,也是每个标签的调用处
  */
object TagContext{
	def main(args:Array[String]):Unit ={
		if( args.length != 4 ) {
			println( "目录参数个数不正确!" )
			sys.exit()
		}

		//模式匹配,给4个参数命名
		var Array( inputPath, docs, stopwords, day ) = args

		//获取spark上下文
		val spark:SparkSession = SparkSession.builder().master( "local[*]" ).appName( this.getClass.getName ).getOrCreate()
		//隐式转换
		import spark.implicits._

		//3.1使用Hbase的准备工作[HBASE_API方面还需要加强]
		// 调用HbaseAPI
		val load = ConfigFactory.load()
		// 获取表名
		val HbaseTableName = load.getString("HBASE.tableName")
		// 创建Hadoop任务
		val configuration = spark.sparkContext.hadoopConfiguration
		// 配置Hbase连接
		configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
		// 获取connection连接
		val hbConn = ConnectionFactory.createConnection(configuration)
		val hbadmin = hbConn.getAdmin

		//3.2Hbase创建表对象,列簇
		// 判断当前表是否被使用
		if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
			println("当前表可用")
			// 创建表对象
			val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
			// 创建列簇
			val hColumnDescriptor = new HColumnDescriptor("tags")
			// 将创建好的列簇加入表中
			tableDescriptor.addFamily(hColumnDescriptor)
			hbadmin.createTable(tableDescriptor)
			hbadmin.close()
			hbConn.close()
		}
		val conf = new JobConf(configuration)
		// 指定输出类型
		conf.setOutputFormat(classOf[TableOutputFormat])
		// 指定输出哪张表
		conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)


		//1.准备工作
		//1.1读取数据文件
		//DataFrame的前身是schemaRDD,可以读取有结构的数据
		val df:DataFrame = spark.read.parquet( inputPath )


		//1.2读取字典文件app_dict
		val docsRDD:RDD[String] = spark.sparkContext.textFile( docs )
		//进行过滤,只留下第1个和第4个参数(从0开始),并返回一个k-v对
		val app_dict:collection.Map[String, String] = docsRDD.map( _.split( "\\s" ) ).filter( _.length >= 5 ).map( arr => {
			(arr( 4 ), arr( 1 ))
		} ).collectAsMap()

		//1.3读取停用字典stopwords,返回一个value为0的kv对
		val banWords:collection.Map[String, Int] = spark.sparkContext.textFile( stopwords ).map( (_, 0) ).collectAsMap()

		//1.4广播两个字典,必须先使用collect方法把数据收集到driver端,再广播
		//使用spark中的sparkContext的broadcast方法,返回一个广播变量
		val broadcast_app_dict:Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast( app_dict )
		val broadcast_banWords:Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast( banWords )


		//2处理数据->打标签
		//调用TagUtil获取用户唯一ID
		df.map( row => {
			val userId:String = TagUtils.getOneUserId( row )
			//2.0 广告类型标签
			val listAd:List[(String, Int)] = TagAdType.makeTags( row )

			//2.1 app名字标签,需要根据app字典转换
			val listAppname:List[(String, Int)] = TagAppname.makeTags( row, broadcast_app_dict )

			//2.2 商圈标签
			val listBusiness:List[(String, Int)] = TagBusiness.makeTags( row )

			//2.3 设备标签
			val listDevice:List[(String, Int)] = TagDevice.makeTags( row )

			//2.4 渠道标签
			val listDitch:List[(String, Int)] = TagDitch.makeTags( row )

			//2.5 关键字标签,需要排除黑名单
			val listKeyword:List[(String, Int)] = TagKeyword.makeTags( row, broadcast_banWords )

			//2.6 地域标签
			val listLocation:List[(String, Int)] = TagLocation.makeTags( row )

			(userId, listAd ++ listAppname ++ listBusiness ++ listDevice ++ listDitch ++ listKeyword ++ listLocation)
		} )
		//根据userId进行聚合,df需要转换为rdd.value是List[(,)]类型
		.rdd.reduceByKey( (list1, list2) => {
			( list1 ::: list2 ).groupBy( _._1 ).mapValues( _.foldLeft[Int]( 0 )( _ + _._2 ) ).toList
		} )
		// 4.存入Hbase
		.map{ case (userId, userTags) => {
			// 设置rowkey和列、列名
			val put = new Put( Bytes.toBytes( userId ) )
			put.addImmutable( Bytes.toBytes( "tags" ), Bytes.toBytes( day ), Bytes.toBytes( userTags.mkString( "," ) ) )
			(new ImmutableBytesWritable(), put)
		}
		       }.saveAsHadoopDataset( conf )

	}
}
