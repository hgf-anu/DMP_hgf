package com.etl

import com.util.String2Type
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class LogInfo (
                     sessionid: String,
                     advertisersid: Int,
                     adorderid: Int,
                     adcreativeid: Int,
                     adplatformproviderid: Int,
                     sdkversion: String,
                     adplatformkey: String,
                     putinmodeltype: Int,
                     requestmode: Int,
                     adprice: Double,
                     adppprice: Double,
                     requestdate: String,
                     ip: String,
                     appid: String,
                     appname: String,
                     uuid: String,
                     device: String,
                     client: Int,
                     osversion: String,
                     density: String,
                     pw: Int,
                     ph: Int,
                     long1: String,
                     lat: String,
                     provincename: String,
                     cityname: String,
                     ispid: Int,
                     ispname: String,
                     networkmannerid: Int,
                     networkmannername:String,
                     iseffective: Int,
                     isbilling: Int,
                     adspacetype: Int,
                     adspacetypename: String,
                     devicetype: Int,
                     processnode: Int,
                     apptype: Int,
                     district: String,
                     paymode: Int,
                     isbid: Int,
                     bidprice: Double,
                     winprice: Double,
                     iswin: Int,
                     cur: String,
                     rate: Double,
                     cnywinprice:Double,
                     imei: String,
                     mac: String,
                     idfa: String,
                     openudid: String,
                     androidid: String,
                     rtbprovince: String,
                     rtbcity: String,
                     rtbdistrict: String,
                     rtbstreet: String,
                     storeurl: String,
                     realip: String,
                     isqualityapp: Int,
                     bidfloor: Double,
                     aw: Int,
                     ah: Int,
                     imeimd5: String,
                     macmd5: String,
                     idfamd5: String,
                     openudidmd5: String,
                     androididmd5: String,
                     imeisha1: String,
                     macsha1: String,
                     idfasha1: String,
                     openudidsha1: String,
                     androididsha1: String,
                     uuidunknow: String,
                     userid: String,
                     iptype: Int,
                     initbidprice: Double,
                     adpayment: Double,
                     agentrate: Double,
                     lomarkrate: Double,
                     adxrate: Double,
                     title: String,
                     keywords: String,
                     tagid: String,
                     callbackdate: String,
                     channelid: String,
                     mediatype: Int
                   )



/*
* 进行数据格式转换
* */
object Log2parquet{

	def main(args:Array[String]):Unit ={
		//1.1设置目录限制
		if( args.length != 2 ) {
			println( "参数不正确,退出程序" )
			sys.exit()
		}
		//1.2获取目录参数,模式匹配
		val Array( inputPath, outputPath ) = args

		//2.1 初始化,设置序列化级别,1.6之前需要sparkconf来配置,2.2之后可以直接使用sparksession
		//第一种方法:
		/*val conf:SparkConf = new SparkConf().setAppName( this.getClass.getName ).setMaster( "local[2]" ).set(
			"spark.serializer",
			"org.apache.spark.serializer.KryoSerializer" )

		val sc = new SparkContext( conf )
		val sQLContext = new SQLContext( sc )*/
		//设置压缩方式
		//sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")

		//第二种方法:
		val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]").set("spark.serializer","org.apache.spark.serializer.KryoSerializer").set("spark.io.compression.codec","Snappy")

		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

		//2.2处理数据
		val ds: Dataset[String] = spark.read.textFile(inputPath)

		val lines: RDD[String] = ds.rdd

		//设置过滤条件和切分条件,内部如果切割条件相连过多,那么需要设置切割处理条件
		//split的第二个参数是t.length或者是-1,效果一样,处理连续的逗号冒号`
		val rawRDD:RDD[Array[String]] = lines.map( t => {
			t.split( ",", -1 )
		})

		val filterRDD:RDD[Array[String]] = rawRDD.filter(_.length >= 85)

		val log:RDD[LogInfo] = filterRDD.map( arr => {
			LogInfo( arr( 0 ),
			        String2Type.toInt( arr( 1 ) ),
			        String2Type.toInt( arr( 2 ) ),
			        String2Type.toInt( arr( 3 ) ),
			        String2Type.toInt( arr( 4 ) ),
			        arr( 5 ),
			        arr( 6 ),
			        String2Type.toInt( arr( 7 ) ),
			        String2Type.toInt( arr( 8 ) ),
			        String2Type.toDouble( arr( 9 ) ),
			        String2Type.toDouble( arr( 10 ) ),
			        arr( 11 ),
			        arr( 12 ),
			        arr( 13 ),
			        arr( 14 ),
			        arr( 15 ),
			        arr( 16 ),
			        String2Type.toInt( arr( 17 ) ),
			        arr( 18 ),
			        arr( 19 ),
			        String2Type.toInt( arr( 20 ) ),
			        String2Type.toInt( arr( 21 ) ),
			        arr( 22 ),
			        arr( 23 ),
			        arr( 24 ),
			        arr( 25 ),
			        String2Type.toInt( arr( 26 ) ),
			        arr( 27 ),
			        String2Type.toInt( arr( 28 ) ),
			        arr( 29 ),
			        String2Type.toInt( arr( 30 ) ),
			        String2Type.toInt( arr( 31 ) ),
			        String2Type.toInt( arr( 32 ) ),
			        arr( 33 ),
			        String2Type.toInt( arr( 34 ) ),
			        String2Type.toInt( arr( 35 ) ),
			        String2Type.toInt( arr( 36 ) ),
			        arr( 37 ),
			        String2Type.toInt( arr( 38 ) ),
			        String2Type.toInt( arr( 39 ) ),
			        String2Type.toDouble( arr( 40 ) ),
			        String2Type.toDouble( arr( 41 ) ),
			        String2Type.toInt( arr( 42 ) ),
			        arr( 43 ),
			        String2Type.toDouble( arr( 44 ) ),
			        String2Type.toDouble( arr( 45 ) ),
			        arr( 46 ),
			        arr( 47 ),
			        arr( 48 ),
			        arr( 49 ),
			        arr( 50 ),
			        arr( 51 ),
			        arr( 52 ),
			        arr( 53 ),
			        arr( 54 ),
			        arr( 55 ),
			        arr( 56 ),
			        String2Type.toInt( arr( 57 ) ),
			        String2Type.toDouble( arr( 58 ) ),
			        String2Type.toInt( arr( 59 ) ),
			        String2Type.toInt( arr( 60 ) ),
			        arr( 61 ),
			        arr( 62 ),
			        arr( 63 ),
			        arr( 64 ),
			        arr( 65 ),
			        arr( 66 ),
			        arr( 67 ),
			        arr( 68 ),
			        arr( 69 ),
			        arr( 70 ),
			        arr( 71 ),
			        arr( 72 ),
			        String2Type.toInt( arr( 73 ) ),
			        String2Type.toDouble( arr( 74 ) ),
			        String2Type.toDouble( arr( 75 ) ),
			        String2Type.toDouble( arr( 76 ) ),
			        String2Type.toDouble( arr( 77 ) ),
			        String2Type.toDouble( arr( 78 ) ),
			        arr( 79 ),
			        arr( 80 ),
			        arr( 81 ),
			        arr( 82 ),
			        arr( 83 ),
			        String2Type.toInt( arr( 84 ) ) )
		} )

		//LogInfo.foreach(println)

		import spark.implicits._

		val df:DataFrame = log.toDF()

		df.write.parquet(outputPath)

		spark.stop()
	}
}

