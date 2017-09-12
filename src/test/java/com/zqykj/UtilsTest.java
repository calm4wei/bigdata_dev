package com.zqykj;

import com.zqykj.bigdata.alert.util.DateUtils;
import com.zqykj.bigdata.alert.util.HBaseUtils;
import com.zqykj.bigdata.hbase.HBaseBaseOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * Created by weifeng on 2017/6/9.
 */
public class UtilsTest {

    HBaseBaseOperator hBaseBaseOperator = null;

    @Before
    public void init() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "172.30.6.81");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase");
        String tableName = "preTest";
        hBaseBaseOperator = new HBaseBaseOperator(config, tableName, true);
    }

    @Test
    public void compareDate() {
        long time = 1494159294737l;
        System.out.println(DateUtils.compare(time, -1));
    }

    @Test
    public void getBeforeTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(1496837694737l));
        calendar.add(Calendar.MONTH, -1);
        System.out.println(calendar.getTime().getTime());
    }

    @Test
    public void testHBaseHashRowkey() {
        byte[] hashBytes = HBaseUtils.hashRowkey("bztDqjhpvmYvJlxuR~`#13002580881@bztDqjhpvmYvJlxuR");
        System.out.println(hashBytes.length);
        for (byte hb : hashBytes) {
            System.out.print(hb + " ,");
        }
        System.out.println();
        System.out.println(new String(hashBytes));
    }

    @Test
    public void testCreateHBaseTable() {

        hBaseBaseOperator.createTable();
    }

    @Test
    public void testHBaseUtil() {
        byte[][] numRegions = hBaseBaseOperator.getHexSplits("00000000", "ffffffff", 100);
        for (int i = 0; i < numRegions.length; i++) {
            System.out.println("num=" + i + " , bytes=" + numRegions[i].length);
            for (int j = 0; j < numRegions[i].length; j++) {
                System.out.print(numRegions[i][j] + ", ");
            }
            System.out.println();
        }

    }

    @Test
    public void testPuts() throws Exception {
        List<Put> putList = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            String uuid = UUID.randomUUID().toString().replaceAll("-", "");
            StringBuilder sb = new StringBuilder();
            sb.append(uuid)
                    .append("~`#13002580881~`#qwerqwerqr@")
                    .append(uuid);
            Put put = new Put(HBaseUtils.hashRowkey(sb.toString()));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(uuid), Bytes.toBytes(sb.toString()));
            putList.add(put);
        }
        HBaseUtils.put("preTest", putList);
    }

}
