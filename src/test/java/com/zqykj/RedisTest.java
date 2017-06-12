package com.zqykj;

import com.zqykj.bigdata.alert.util.RedisProvider;
import org.junit.Test;

/**
 * Created by weifeng on 2017/6/12.
 */
public class RedisTest {

    @Test
    public void delRedisKey() {
        RedisProvider.batchDel("w");
    }
}
