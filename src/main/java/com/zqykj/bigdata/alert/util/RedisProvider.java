package com.zqykj.bigdata.alert.util;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

public class RedisProvider {
    protected static final Logger LOG = LoggerFactory.getLogger(RedisProvider.class);
    protected static JedisPool jedispool;

    public static void initRedisContext(SparkConf sparkConf) {

        JedisPoolConfig jedisconfig = new JedisPoolConfig();
        jedisconfig.setMinIdle(sparkConf.getInt("redis.pool.min.idle", 2));
        jedisconfig.setMaxTotal(sparkConf.getInt("redis.pool.max.total", 50));

        jedispool = new JedisPool(jedisconfig,
                sparkConf.get("redis.ip", "192.168.0.63"),
                sparkConf.getInt("redis.port", 6379),
                sparkConf.getInt("redis.timeout", 10000));
    }

    public static Jedis getJedis() {
        boolean broken = false;
        Jedis jedis = null;
        try {
            jedis = jedispool.getResource();
        } catch (JedisException je) {
            // 是否销毁与回收对象
            broken = handleJedisException(je);
        } finally {
            // 回收到连接池
            closeResource(jedis, broken);
        }
        return jedis;
    }

    public static void returnResource(JedisPool pool, Jedis jedis) {
        if (jedis != null) {
            pool.returnResource(jedis);
        }
    }

    /**
     * 读取所有key
     *
     * @return
     */
    public static Set getKeys() {
        Set set = null;
        try {
            set = getJedis().keys("*");
        } catch (Exception e) {

        }
        return set;
    }

    public static String get(String key) {
        String str = null;
        try {
            str = getJedis().get(key);
        } catch (Exception e) {

        }
        return str;
    }

    public static String hGet(String key, String field) {
        return getJedis().hget(key, field);
    }

    /**
     * 模糊key查询
     *
     * @param key
     * @return
     */
    public static Set getKeys(String key) {
        Set set = null;
        try {
            set = getJedis().keys(key + "*");
        } catch (Exception e) {

        }
        return set;
    }

    /**
     * Map集合
     *
     * @param key
     * @param map
     */
    public static void hmSet(String key, Map map) {
        try {
            getJedis().hmset(key, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 缓存List
     *
     * @param key
     * @param value
     */
    public static void addList(String key, String... value) {
        try {
            getJedis().rpush(key, value);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 全取List
     *
     * @param key
     * @return
     */
    public static List<String> getList(String key) {

        try {
            return getJedis().lrange(key, 0, -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 读取单个Key集合
     *
     * @param key
     * @param mvalue
     * @return
     */
    public static List<String> hmGet(String key, String... mvalue) {
        List<String> strings = null;
        try {
            Jedis jedis = getJedis();
            strings = jedis.hmget(key, mvalue);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return strings;
    }

    /**
     * set 键值
     *
     * @param key
     * @param value
     */
    public static void set(String key, String value) {
        try {
            getJedis().set(key, value);
        } catch (Exception e) {

        }
    }

    /**
     * 带过期时间
     *
     * @param key
     * @param time
     * @param value
     */
    public static void set(String key, int time, String value) {
        try {
            getJedis().setex(key, time, value);
        } catch (Exception e) {

        }
    }

    /**
     * 删除指定key
     *
     * @param key
     */
    public static void del(String... key) {
        try {
            getJedis().del(key);
        } catch (Exception e) {

        }
    }

    public static void batchDel(String key) {
        try {
            Set<String> set = getJedis().keys(key + "*");
            Iterator<String> it = set.iterator();
            while (it.hasNext()) {
                String keyStr = it.next();
                // System.out.println(keyStr);
                getJedis().del(keyStr);
            }
        } catch (Exception e) {

        }
    }

    /**
     * 删除map中指定值
     *
     * @param key
     * @param mvalue
     */
    public static void hDel(String key, String... mvalue) {
        try {
            getJedis().hdel(key, mvalue);
        } catch (Exception e) {

        }
    }

    protected static boolean handleJedisException(JedisException jedisException) {
        if (jedisException instanceof JedisConnectionException) {
            LOG.error("Redis connection lost.", jedisException);
        } else if (jedisException instanceof JedisDataException) {
            if ((jedisException.getMessage() != null) && (jedisException.getMessage().indexOf("READONLY") != -1)) {
                LOG.error("Redis connection are read-only slave.", jedisException);
            } else {
                // dataException, isBroken=false
                return false;
            }
        } else {
            LOG.error("Jedis exception happen.", jedisException);
        }
        return true;
    }

    protected static void closeResource(Jedis jedis, boolean conectionBroken) {
        try {
            if (conectionBroken) {
                jedispool.returnBrokenResource(jedis);
            } else {
                jedispool.returnResource(jedis);
            }
        } catch (Exception e) {
            LOG.error("return back jedis failed, will fore close the jedis.", e);
        }
    }
}