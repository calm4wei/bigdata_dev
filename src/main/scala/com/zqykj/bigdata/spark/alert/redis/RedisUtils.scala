package com.zqykj.bigdata.spark.alert.redis

import java.util

import com.alibaba.fastjson.JSONArray
import com.zqykj.bigdata.alert.entity.UFlag
import com.zqykj.bigdata.alert.util.RedisProvider
import com.zqykj.bigdata.spark.alert.common.GlobalRedisPool

/**
  * Created by weifeng on 2017/6/9.
  */
object RedisUtils {

	def hGet(hKey: String, entityType: String): String = {
		val jedis = GlobalRedisPool().getResource
		GlobalRedisPool().returnResource(jedis)
		jedis.hget(hKey, entityType)
	}

	def insert(hkey: String, entityType: String, list: util.List[UFlag]): Unit = {
		val jedis = GlobalRedisPool().getResource
		GlobalRedisPool().returnResource(jedis)
		val jsonArray = new JSONArray()
		import scala.collection.JavaConversions._
		for (u <- list) {
			jsonArray.add(u)
		}
		val map = new util.HashMap[String, String]()
		map.put(entityType, jsonArray.toJSONString)
		jedis.hmset(hkey, map)
	}

	def insert(hKey: String, entityType: String, uFlag: UFlag): Unit = {
		val jedis = GlobalRedisPool().getResource
		GlobalRedisPool().returnResource(jedis)
		val jsonArray = new JSONArray()
		jsonArray.add(uFlag)
		val map = new util.HashMap[String, String]()
		map.put(entityType, jsonArray.toJSONString)
		jedis.hmset(hKey, map)
	}

}
