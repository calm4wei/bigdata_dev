package com.zqykj.bigdata

import com.zqykj.bigdata.alert.util.RedisProvider.hmGet

/**
  * Created by weifeng on 2017/6/8.
  */
object GatherTest {

  def main(args: Array[String]): Unit = {
    val elements = hmGet("geohash@testELP", "testCase2-3")
    println(Option(elements.get(0)).isEmpty)
  }

}
