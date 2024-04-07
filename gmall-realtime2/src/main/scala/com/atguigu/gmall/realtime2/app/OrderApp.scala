package com.atguigu.gmall.realtime2.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime2.bean.OrderInfo
import com.atguigu.gmall.realtime2.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("OrderApp")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)
    val orderInfoStream = sourceStream.map(s => JSON.parseObject(s, classOf[OrderInfo]))
    orderInfoStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "gmall_order_info",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Some("hadoop104,hadoop106,hadoop108:2181")
      )
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
