package com.zqykj.bigdata.alert.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by weifeng on 2017/6/7.
 */
public class JedisDataSourceUtils {

    private static JedisPool jedisPool = null;

    public static JedisPool getInstance() {
        if (null == jedisPool) {
            synchronized (JedisDataSourceUtils.class) {
                if (null == jedisPool) {
                    // 主机地址
                    String host = "192.168.1.100";
                    Integer port = 6379;
                    // 授权密码
                    String password = "123456";
                    // 超时时间
                    int timeout = 10000;

                    jedisPool = new JedisPool(initJedisConfig(), host, port, timeout, password);
                }
            }
        }
        return jedisPool;
    }

    private static JedisPoolConfig initJedisConfig() {
        // 构建连接池配置信息
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 设置最大连接数
        jedisPoolConfig.setMaxTotal(50);
        jedisPoolConfig.setMinIdle(3);
        return jedisPoolConfig;
    }

    public static Jedis getJedis() {
        return getInstance().getResource();
    }

    public void close(Jedis jedis) {
        if (null != jedis) {
            jedisPool.returnResource(jedis);
        }
    }

}
