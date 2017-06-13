package com.zqykj.bigdata

import java.util
import java.util.{Date, UUID}

import com.alibaba.fastjson.JSONArray
import com.zqykj.bigdata.alert.entity.{CongestWarningEvent, UFlag}
import com.zqykj.bigdata.alert.util.RedisProvider._

/**
  * Created by weifeng on 2017/6/8.
  */
object GatherTest {

	def main(args: Array[String]): Unit = {
		getEntityEventBytes()
		getUFlag()
	}


	def foreachList(list: util.List[String]): Unit = {
		println(list.size)
		if (list != null && list.get(0) != null) {
			import scala.collection.JavaConversions._
			for (s <- list) {
				println(s)
			}
		}
	}

	def getUFlag(): UFlag = {
		val uid = UUID.randomUUID().toString.replaceAll("-", "")
		val uFlag = new UFlag(uid, System.currentTimeMillis())
		println(s"uFlag size=${uFlag.toString.getBytes().size} , ${uFlag.toString}")
		uFlag
	}

	def getEntityEventBytes(): Unit = {
		val event = new CongestWarningEvent
		event.setEventId(UUID.randomUUID().toString)
		event.setWarningType("congestS")
		event.setOrigineTime(System.currentTimeMillis())
		event.setProduceTime(System.currentTimeMillis())
		event.setEventTime(new Date().getTime)
		event.setInElpId("testELP")
		event.setInTypeId("testCase2-1")
		val uid = UUID.randomUUID().toString.replaceAll("-", "")
		event.setInUid(uid)
		event.setInLabel(uid)
		event.setGeohashString("ws10f")
		val list = new util.ArrayList[UFlag]()
		list.add(getUFlag())
		list.add(getUFlag())
		event.setFlagList(list)
		println(s"CongestWarningEvent unique size = ${event.toString.getBytes.size}, ${event.toString}")

	}

}
