package com.atguigu.gmall.realtime2.util

import java.util.Properties



object ConfigUtil {
  val is = ConfigUtil.getClass.getClassLoader.getResourceAsStream("config.properties")
  private val prop = new Properties()
  prop.load(is)
  def getProperty(key:String)={
      prop.getProperty(key)
  }

  def main(args: Array[String]): Unit = {
    println(ConfigUtil.getProperty("kafka.servers"))
  }
}
