package com.zqykj.bigdata.spark.nifi.streaming

import com.zqykj.bigdata.spark.LoggerLevels
import org.apache.log4j.Level
import org.apache.nifi.remote.client.{SiteToSiteClient, SiteToSiteClientConfig}
import org.apache.nifi.spark.NiFiReceiver
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by weifeng on 2017/6/20.
  */
object NiFiSparkReceiverDemo01 {


	def main(args: Array[String]): Unit = {

		LoggerLevels.setStreamingLogLevels(Level.WARN)

		val config = new SiteToSiteClient.Builder().url("http://localhost:8080/nifi").portName("Data For Spark").buildConfig
		val sparkConf = new SparkConf()
			.setAppName("NiFi-Spark Streaming example")
			.setMaster("local[4]")

		val ssc = new StreamingContext(sparkConf, Seconds(2))
		val packetStream = ssc.receiverStream(new NiFiReceiver(config, StorageLevel.MEMORY_ONLY))
		val text = packetStream.map(packet => packet.getAttributes.get("uuid"))
		text.print(10)

		ssc.start()
		ssc.awaitTermination()

	}

}
