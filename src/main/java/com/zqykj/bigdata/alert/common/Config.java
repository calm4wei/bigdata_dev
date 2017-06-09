package com.zqykj.bigdata.alert.common;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by weifeng on 2017/6/9.
 */
public class Config {

    public static String configPath = "conf/zqy-app.properties";
    public static Properties CONFIG = null;

    static {
        CONFIG = new Properties();
        try {
            CONFIG.load(new FileReader(configPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
