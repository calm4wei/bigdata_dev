package com.zqykj.bigdata

import com.zqykj.bigdata.spark.alert.job.InOutJob
import org.junit.Test

/**
  * Created by alfer on 7/27/17.
  */
class InOutTest {

  @Test
  def inOutTest(): Unit = {
    val job = new InOutJob
    job.hello("hell Jack")
  }

  @Test
  def test(): Unit = {
    print("test")
  }

  @Test
  def testOne(): Unit = {
    println("testOne")
  }

}
