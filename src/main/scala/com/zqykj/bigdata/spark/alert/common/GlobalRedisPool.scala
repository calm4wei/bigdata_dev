package com.zqykj.bigdata.spark.alert.common

import org.apache.spark.Logging
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by weifeng on 2017/6/9.
  */
object GlobalRedisPool extends Serializable with Logging {

	// sys.addShutdownHook(close())

	/**
	  * The Redis Pool
	  */
	private lazy val redisPool = {
		// 构建连接池配置信息
		val jedisPoolConfig = new JedisPoolConfig
		// 设置最大连接数
		jedisPoolConfig.setMaxTotal(50)
		jedisPoolConfig.setMinIdle(3)

		// 主机地址
		val host = "192.168.0.63"
		val port = 6379
		// 授权密码
		//val password = "123456"
		// 超时时间
		val timeout = 10000

		// 使用 dbIndex 0
		new JedisPool(jedisPoolConfig, host, port, timeout, null, 0)

	}

	def apply(): JedisPool = redisPool

	/**
	  * Close HBase connection
	  */
	def close(): Unit = if (!redisPool.isClosed) {
		logInfo("Closing the global Redis Pool")
		redisPool.close()
		logInfo("The global Redis Pool closed")
	}

}
