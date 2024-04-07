package com.atguigu.gmall.realtime2.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.{Constant, ESTUtil}
import com.atguigu.gmall.realtime2.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall.realtime2.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.tools.cmd.Property

object SaleDetailApp1 {


  def saveTORedis(client: Jedis, key: String, value: AnyRef) = {
    import org.json4s.DefaultFormats
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
     orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions(it =>{
       val client = RedisUtil.getClient()
       val result = it.flatMap {
         case (orderId, (Some(orderInfo), opt)) =>

           cacheOrderInfo(client,orderInfo)

           val keys = client.keys(s"order_detail:${orderId}:*").toList

           keys.map(key =>{

             val orderDetailString = client.get(key)
             client.del(key)//防止orderDetail被重复join
             val orderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
             SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)

           }) ::: (opt match {
             case Some(orderDetail) =>
               SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)::Nil
             case None => Nil
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



       }
       client.close()
       result
     })
  }
  //使用spark-sql读取mysql中user_info信息

  def joinUser(saleDetailStream: DStream[SaleDetail], ssc: StreamingContext) = {
    val url = "jdbc:mysql://hadoop108:3306/gmall"
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val spark = SparkSession.builder()
      .config(ssc.sparkContext.getConf)
      .getOrCreate()
    import spark.implicits._
  saleDetailStream.transform((saleDetailRdd: RDD[SaleDetail]) => {
      val userInfoRDD = spark.read.jdbc(url, "user_info", props)
        .as[UserInfo]
        .rdd
        .map(user => (user.id, user))
      saleDetailRdd.map(saleDetail => (saleDetail.user_id, saleDetail))
        .join(userInfoRDD)
        .map {
          case (user_id, (saleDetail, userInfo)) =>
            saleDetail.mergeUserInfo(userInfo)
        }
    })

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
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
    var saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)
    //4.根据用户的id反查mysql中的user_info,得到用户生日和性别
    saleDetailStream= joinUser(saleDetailStream,ssc)
   saleDetailStream.foreachRDD(rdd =>{
     ESTUtil.insertBulk("sale_detail",rdd.collect().toIterator)
   })
    saleDetailStream.print(1000)
    //5.把详情写到es中
    ssc.start()
    ssc.awaitTermination()
  }
}
