package com.atguigu.gmall.realtime2.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.{Constant, ESTUtil, ESTUtil2}
import com.atguigu.gmall.realtime2.bean.{AlertInfo, EventLog}
import com.atguigu.gmall.realtime2.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import java.util

import scala.util.control.Breaks._



object AlertApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AlertApp")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = MyKafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_EVENT)
      .window(Minutes(5),Seconds(6))
    val eventLogStream = sourceStream.map(eventLog => JSON.parseObject(eventLog, classOf[EventLog]))
    val eventLogGroup = eventLogStream
      .map(eventLog => (eventLog.mid, eventLog))
      .groupByKey()
    val alertInfoStream = eventLogGroup.map {
      case (mid, eventLogs) =>
        //当前设备的所有用户
        val uidSet = new util.HashSet[String]()
        //所有事件
        val eventList = new util.ArrayList[String]()
        //领优惠券对应的产品ID
        val itemSet = new util.HashSet[String]()

        var isClickItem = false
        breakable {
          eventLogs.foreach(log => {
            eventList.add(log.eventId)
            log.eventId match {
              case "coupon" =>
                uidSet.add(log.uid)
                itemSet.add(log.itemId)
              case "clickItem" =>
                isClickItem = true
                break
              case _ =>
            }
          })
        }
        //
        (uidSet.size() > 3 && !isClickItem, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }
    //预警信息写入ES
    alertInfoStream
        .filter(_._1)
        .map(_._2)
        .foreachRDD(rdd =>{
          rdd.foreachPartition((alertInfos: Iterator[AlertInfo]) =>{
            val data = alertInfos.map(info => (info.mid + ":" + info.ts / 1000 / 60, info))
            ESTUtil2.insertBulk(Constant.INDEX_ALERT,data)
          })
        })
    alertInfoStream.print(1000)
    ssc.start()
    ssc.awaitTermination()
  }
}
