package com.zqykj.bigdata.alert.entity;

import com.alibaba.fastjson.JSON;

import java.util.List;

/**
 * Created by weifeng on 2017/6/7.
 */
public class CongestWarningEvent {

    private String eventId;
    private String warningType;
    private Long origineTime;
    private Long produceTime;
    private Long eventTime;
    private String inElpId;
    private String inTypeId;
    private String inUid;
    private String inLabel;
    private String geohashString;
    private List<UFlag> flagList;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getWarningType() {
        return warningType;
    }

    public void setWarningType(String warningType) {
        this.warningType = warningType;
    }

    public Long getOrigineTime() {
        return origineTime;
    }

    public void setOrigineTime(Long origineTime) {
        this.origineTime = origineTime;
    }

    public Long getProduceTime() {
        return produceTime;
    }

    public void setProduceTime(Long produceTime) {
        this.produceTime = produceTime;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getInElpId() {
        return inElpId;
    }

    public void setInElpId(String inElpId) {
        this.inElpId = inElpId;
    }

    public String getInTypeId() {
        return inTypeId;
    }

    public void setInTypeId(String inTypeId) {
        this.inTypeId = inTypeId;
    }

    public String getInUid() {
        return inUid;
    }

    public void setInUid(String inUid) {
        this.inUid = inUid;
    }

    public String getInLabel() {
        return inLabel;
    }

    public void setInLabel(String inLabel) {
        this.inLabel = inLabel;
    }

    public String getGeohashString() {
        return geohashString;
    }

    public void setGeohashString(String geohashString) {
        this.geohashString = geohashString;
    }

    public List<UFlag> getFlagList() {
        return flagList;
    }

    public void setFlagList(List<UFlag> flagList) {
        this.flagList = flagList;
    }
}
