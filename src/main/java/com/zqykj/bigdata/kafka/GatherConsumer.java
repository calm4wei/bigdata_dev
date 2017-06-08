package com.zqykj.bigdata.kafka;

/**
 * Created by weifeng on 2017/6/7.
 */
public class GatherConsumer {

    public static void main(String[] args) {

        Consumer consumer = new Consumer(new String[]{"detected"});
        consumer.consumerData();
    }
}
