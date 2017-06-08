package com.zqykj.geohash;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zqykj.bigdata.alert.entity.UFlag;
import com.zqykj.bigdata.alert.util.JedisDataSourceUtils;
import com.zqykj.bigdata.alert.util.RedisProvider;
import com.zqykj.job.geo.utils.GeoHash;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by weifeng on 2017/6/7.
 */
public class GeoHashTest {

    @Test
    public void getGeohash() {
        String latitude = "22.49084663";
        String longitude = "113.93402863";
        long t1 = System.currentTimeMillis();
        System.out.println(GeoHash.encodeGeohash(Double.valueOf(latitude)
                , Double.valueOf(longitude)
                , 5));
        long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);

    }

    @Test
    public void setHkeyHash() {
        Jedis jedis = new Jedis("wf-vm", 6379);
        jedis.auth("123456");
        Map<String, String> hash = new HashMap<>();
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(new UFlag("uid", System.currentTimeMillis()));
        jsonArray.add(new UFlag("uid2", System.currentTimeMillis()));
        hash.put("testCase2-1", jsonArray.toJSONString());
        jedis.hmset("geohash@testELP", hash);
    }

    @Test
    public void getHkeyHash() {
        Jedis jedis = new Jedis("wf-vm", 6379);
        jedis.auth("123456");
        List<String> list = jedis.hmget("geohash@testELP", "testCase2-1");
        foreachList(list);
    }

    @Test
    public void getHkeyHashByRedisProvider() {
        List<String> list = RedisProvider.hmGet("geohash@testELP", "testCase2-3");
        foreachList(list);
    }

    public void foreachList(List<String> list) {
        System.out.println(list.size());
        if (list != null && list.get(0) != null) {
            for (String s : list) {
                System.out.println(s);
                System.out.println(JSONArray.parseArray(s).size());
            }
        }

    }

    @Test
    public void getHKeyHashByPool() {
        Jedis jedis = JedisDataSourceUtils.getJedis();

        JSONArray jsonArray = new JSONArray();
        jsonArray.add(new UFlag(UUID.randomUUID().toString().replaceAll("-", "")
                , System.currentTimeMillis()));
        Map<String, String> hash = new HashMap<>();
        hash.put("testCase2-2", jsonArray.toJSONString());
        jedis.hmset("geohash@testELP", hash);

        List<String> list = jedis.hmget("geohash@testELP", "testCase2-1", "testCase2-2");
        foreachList(list);
        jedis.close();
    }


}
