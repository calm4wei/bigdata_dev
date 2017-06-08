package com.zqykj.bigdata.alert.entity;

import com.alibaba.fastjson.JSON;

/**
 * Created by weifeng on 2017/6/7.
 */
public class UFlag {

    private String uid;
    private Long timestamp;

    public UFlag() {
    }

    public UFlag(String uid, Long timestamp) {
        this.uid = uid;
        this.timestamp = timestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
