package com.zqykj.bigdata.alert.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by alfer on 8/15/17.
 */
public class HBaseUtils {

    private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);
    private static Configuration conf = null;
    private static Connection conn = null;

    static {
        try {
            if (conf == null) {
                conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "172.30.6.81");
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                conf.set("zookeeper.znode.parent", "/hbase");
            }
        } catch (Exception e) {
            logger.error("HBase Configuration Initialization failure !");
            throw new RuntimeException(e);
        }
    }

    /**
     * 获得链接
     *
     * @return
     */
    public static synchronized Connection getConnection() {
        try {
            if (conn == null || conn.isClosed()) {
                conn = ConnectionFactory.createConnection(conf);
            }
        } catch (IOException e) {
            logger.error("HBase 建立链接失败 ", e);
        }
        return conn;

    }


    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void closeConnect(Connection conn) {
        if (null != conn) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("closeConnect failure !", e);
            }
        }
    }

    /**
     * 建表
     *
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    private static void createTable(String tableName, String[] cfs, byte[][] splitKeys) throws Exception {
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                logger.warn("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < cfs.length; i++) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs[i]);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hColumnDescriptor);
            }
            admin.createTable(tableDesc, splitKeys);
            logger.info("Table: {} create success!", tableName);
        } finally {
            admin.close();
            closeConnect(conn);
        }
    }

    /**
     * 建表
     *
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    private static void createTable(String tableName, String[] cfs) throws Exception {
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                logger.warn("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < cfs.length; i++) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs[i]);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hColumnDescriptor);
            }
            admin.createTable(tableDesc);
            logger.info("Table: {} create success!", tableName);
        } finally {
            admin.close();
            closeConnect(conn);
        }
    }


    public static void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
        if (preBuildRegion) {
            String[] s = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};
            int partition = 16;
            byte[][] splitKeys = new byte[partition - 1][];
            for (int i = 1; i < partition; i++) {
                splitKeys[i - 1] = Bytes.toBytes(s[i - 1]);
            }
            createTable(tableName, columnFamilies, splitKeys);
        } else {
            createTable(tableName, columnFamilies);
        }
    }

    /**
     * 异步往指定表添加数据
     *
     * @param tablename 表名
     * @param puts      需要添加的数据
     * @return long                返回执行时间
     * @throws IOException
     */
    public static long put(String tablename, List<Put> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection();
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    System.out.println("Failed to sent put " + e.getRow(i) + ".");
                    logger.error("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
                .listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

        final BufferedMutator mutator = conn.getBufferedMutator(params);
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
            closeConnect(conn);
        }
        return System.currentTimeMillis() - currentTime;
    }

    public static byte[] hashRowkey(String rowkey) {
        byte[] rowkeyBytes = Bytes.toBytes(rowkey);
        return Bytes.add(MD5Hash.getMD5AsHex(rowkeyBytes).substring(0, 8).getBytes(), rowkeyBytes);
    }
}
