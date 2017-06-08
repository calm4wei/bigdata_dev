package com.zqykj.bigdata.alert.entity;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * Created by zqykj on 2017/5/16.
 */
public class DetectedData implements Serializable {

    private String uid;
    private String elpID;
    private String label;
    private String entity_type;
    private String entity_id;
    private long timestamp;

    /**
     * 纬度
     */
    private String latitude;
    private String longitude;

    public DetectedData() {
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public String getElpID() {
        return elpID;
    }

    public void setElpID(String elpID) {
        this.elpID = elpID;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getEntity_type() {
        return entity_type;
    }

    public void setEntity_type(String entity_type) {
        this.entity_type = entity_type;
    }

    public String getEntity_id() {
        return entity_id;
    }

    public void setEntity_id(String entity_id) {
        this.entity_id = entity_id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }
}
