package com.atguigu.gmall.common

import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}

object ESTUtil2 {
  val esurl ="Http://hadoop106:9200"
  val factory = new JestClientFactory
  val conf = new HttpClientConfig.Builder(esurl)
    .multiThreaded(true)
    .maxTotalConnection(100)
    .connTimeout(10*1000)
    .readTimeout(10*1000)
    .build()
  factory.setHttpClientConfig(conf)
  def getClient = factory.getObject
  def insertSingle(action:String,source:Object,id:String=null)={
    val client = factory.getObject
    val index = new Index.Builder(source)
      .index(action)
      .`type`("_doc")
      .id(id)//可选id是null相当没有传
      .build()
    client.execute(index)
    client.shutdownClient()
  }
  def insertBulk(index:String,sources:Iterator[Any])={

    val client = factory.getObject
    val bulk = new Bulk.Builder().defaultIndex(index).defaultType("_doc")
    /*      sources.foreach(source =>{
            val action = new Index.Builder(source).build()
            bulk.addAction(action)
          })*/
    sources.foreach{
      case (id:String,data) =>
        val action = new Index.Builder(data).id(id).build()
        bulk.addAction(action)
      case data =>
        val action = new Index.Builder(data).build()
        bulk.addAction(action)
    }
    client.execute(bulk.build())
    client.shutdownClient()
  }
  /*  def insertBulk(): Unit = {
      val user1 = User(70,"hanwuji")
      val action1 = new Index.Builder(user1).build()
      val user2 = User(88,"qingshihuang")
      val action2 = new Index.Builder(user2).build()
      val client = factory.getObject
      val bulk = new Bulk.Builder()
        .defaultIndex("user")
        .defaultType("_doc")
        .addAction(action1)
        .addAction(action2)
        .build()
      client.execute(bulk)
      client.shutdownClient()
    }*/

  /*def main(args: Array[String]): Unit = {
/*    val list1 = User(40,"a")::User(50,"b")::User(60,"c")::Nil
    val list2 = ("100",User(700,"a"))::("200",User(800,"b"))::("300",User(900,"c"))::Nil
    insertBulk("user",list2.toIterator)*/
//    val data = User(66,"dada")
//    insertSingle("user",data,"3")
//    insertBulk()

/*    val data=
      """{
        |"name":"lisi",
        |"age":18
        |}""".stripMargin*/
//    val data = User(50,"da")


  }*/
}
