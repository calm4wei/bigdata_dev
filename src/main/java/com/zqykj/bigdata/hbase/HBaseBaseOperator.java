package com.zqykj.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by alfer on 8/15/17.
 */
public class HBaseBaseOperator {

    private static Logger logger = LoggerFactory.getLogger(HBaseBaseOperator.class);
    private Configuration config;
    private String tableName;
    private String startKey;
    private String endKey;
    private boolean isPreSplit = true;

    private static final String cf = "f";
    private static final Integer numRegions = 10;


    public HBaseBaseOperator(final Configuration config, String tableName, boolean isPreSplit) {
        this.config = config;
        this.tableName = tableName;
        this.isPreSplit = isPreSplit;
        this.startKey = "0000000000000000";
        this.endKey = "ffffffffffffffff";
    }

    public void createTable() {
        Connection connection = null;
        Admin admin = null;
        TableName table = TableName.valueOf(this.tableName);
        try {
            connection = ConnectionFactory.createConnection(this.config);
            admin = connection.getAdmin();
            if (admin.tableExists(table)) {
                logger.error("table: " + tableName + " has exist");
                admin.disableTable(table);
                admin.deleteTable(table);
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(table);
            tableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(cf)));

            if (isPreSplit) {
                admin.createTable(tableDescriptor, getHexSplits(startKey, endKey, numRegions));
            } else {
                admin.createTable(tableDescriptor);

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }


}
