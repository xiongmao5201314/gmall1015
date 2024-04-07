package com.atguigu.gmall.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject

import scala.collection.JavaConversions._
import com.alibaba.otter.canal.client.CanalConnectors
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.atguigu.gmall.common.Constant

import scala.util.Random



object CanalClient {



  def main(args: Array[String]): Unit = {
    //连接canal
    val address = new InetSocketAddress("hadoop108", 11111)
    val connector = CanalConnectors.newSingleConnector(
      address, "example", "", "")
    connector.connect()
    //订阅数据
    connector.subscribe("gmall.*")

    while(true){

      val msg = connector.get(100)//一次从canal中拉取100条数据变化
      val entriesOption =if(msg != null) Some(msg.getEntries) else None
      if (entriesOption.isDefined && entriesOption.get.nonEmpty ) {

        val entries = entriesOption.get
        println(entries.nonEmpty)
        for (entry <- entries){

          if(entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA){
            val storeValue = entry.getStoreValue
            val rowChange = RowChange.parseFrom(storeValue)
            val rowDatas = rowChange.getRowDatasList

            handleData(entry.getHeader.getTableName,rowDatas,rowChange.getEventType)
          }

        }

      }else{
        println("没有拉取到数据，2s之后重试中......")
        Thread.sleep(2000)
      }


    }
    def handleData(tableName: String, rowDatas: util.List[CanalEntry.RowData], eventType: CanalEntry.EventType)={
      if("order_info" == tableName && eventType == EventType.INSERT && rowDatas != null && rowDatas.nonEmpty){

        sendToKafka(Constant.TOPIC_ORDER_INFO,rowDatas)
      }else if("order_detail" == tableName && eventType == EventType.INSERT && rowDatas != null && rowDatas.nonEmpty){
        sendToKafka(Constant.TOPIC_ORDER_DETAIL,rowDatas)
      }

    }
  }

  private def sendToKafka(topic:String,rowDatas: util.List[CanalEntry.RowData]) = {
    for (rowData <- rowDatas) {
      val result = new JSONObject()
      val columnsList = rowData.getAfterColumnsList
      for (column <- columnsList) {
        val key = column.getName
        val value = column.getValue
        result.put(key, value)
      }
//      KafakaUtil.send(topic, result.toJSONString)
//      println(result.toJSONString)
      new Thread(){
        override def run(): Unit = {
          Thread.sleep(new Random().nextInt(20*1000))
          KafakaUtil.send(topic, result.toJSONString)
        }
      }.start()//模拟延迟
    }
  }
}
