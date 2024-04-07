package com.atguigu.gmall.realtime2.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime2.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall.realtime2.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.GenTraversableOnce

object SaleDetailApp {


  def saveTORedis(client: Jedis, key: String, value: AnyRef) = {
    import org.json4s.DefaultFormats
    //
    val json = Serialization.write(value)(DefaultFormats)
//    client.set(key,json)
    client.setex(key,60*30,json)
  }


  def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo) = {
    val key = "order_info:" + orderInfo.id
    saveTORedis(client,key,orderInfo)
  }

  def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail) = {
    val key = s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
    saveTORedis(client,key,orderDetail)
  }
  def fullJoin(orderInfoStream: DStream[(String, OrderInfo)],
               orderDetailStream: DStream[(String, OrderDetail)]) = {
    import scala.collection.JavaConversions._
    //val value: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoStream.fullOuterJoin(orderDetailStream)
    orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions(it =>{
       val client = RedisUtil.getClient()
       val result = it.flatMap {
         case (orderId, (Some(orderInfo), Some(orderDetail))) =>
           println("some","some")
           cacheOrderInfo(client,orderInfo)
           val first = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
           val keys = client.keys(s"order_detail:${orderId}:*").toList

           first :: keys.map(key =>{

             val orderDetailString = client.get(key)
             client.del(key)//防止orderDetail被重复join
             val orderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
             SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)

           })

         case (orderId, (None, Some(orderDetail))) =>
           println("none","some")
          val orderInfoString = client.get("order_info:" + orderId)
           if(orderInfoString != null && orderInfoString.nonEmpty){
             val orderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
             SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)::Nil
           }else{
             cacheOrderDetail(client,orderDetail)
             Nil
           }
         case (orderId, (Some(orderInfo), None)) =>
           println("some","none")
           cacheOrderInfo(client,orderInfo)
           val keys = client.keys(s"order_detail:${orderId}:*").toList
           keys.map(key =>{
             val orderDetailString = client.get(key)
             client.del(key)
             val orderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
             SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)

           })


       }
       client.close()
       result
     })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc = new StreamingContext(conf, Seconds(3))
    //1.读取kafka的2个topic流
    //2.对他们做封装
    val orderInfoStream = MyKafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)
      .map(s => {
        val orderInfo = JSON.parseObject(s, classOf[OrderInfo])
        (orderInfo.id, orderInfo) //join必须k,v形式
      })
    val orderDetailStream = MyKafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_ORDER_DETAIL)
      .map(s => {
        val orderDetail = JSON.parseObject(s, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail) //join必须k,v形式
      })
//    orderInfoStream.print(1000)
//
//    orderDetailStream.print(1000)



    //3.双流join
    val saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)
    //4.根据用户的id反查mysql中的user_info,得到用户生日和性别
    saleDetailStream.print(1000)

    //5.把详情写到es中
    ssc.start()
    ssc.awaitTermination()
  }
}
