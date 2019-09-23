package com.util

/**
  * 处理指标统计工具类
  */
object RptUtil{
	//1.处理请求数的函数,参数是requestmode,processnode.
	// 注意:如果后面条件成立了前面的条件也自然成立
	def repPt(requestmode:Int, processnode:Int):List[Double] ={
		if( requestmode == 1 && processnode == 1 ) {
			//只有原始请求的情况,processnode>=1
			List( 1, 0, 0 )
		} else if( requestmode == 1 && processnode == 2 ) {
			//是有效请求,自然也是原始请求,processnode>=2
			List( 1, 1, 0 )
		} else if( requestmode == 1 && processnode == 3 ) {
			//广告请求数,前面两个也满足,processnode=3
			List( 1, 1, 1 )
		} else {
			//都不是的情况返回3个0
			List( 0, 0, 0 )
		}
	}

	//2.处理点击展示数
	def clickPt(requestmode:Int, iseffective:Int):List[Double] ={
		if( requestmode == 2 && iseffective == 1 ) {
			List( 1, 0 )
		} else if( requestmode == 3 && iseffective == 1 ) {
			List( 0, 1 )
		} else {
			List( 0, 0 )
		}
	}


	//3.处理竞价,成功数广告成品和消费
	def adPt(iseffective:Int,
	         isbilling  :Int,
	         isbid      :Int,
	         iswin      :Int,
	         adordeerid :Int,
	         winprice   :Double,
	         adpayment  :Double):List[Double] ={
		if(iseffective==1&&isbilling==1&& isbid==1){
			List(1,0,0,0)
		}else if(iseffective==1&&isbilling==1&&iswin==1&&adordeerid!=0){
			List(1,1,winprice/1000,adpayment/1000)
		}else{
			List(0,0,0,0)
		}
	}


}
