package com.zqykj.bigdata.alert.entity;

import java.util.List;

/**
 * Created by weifeng on 2017/6/7.
 */
public class CongestWarningEvent {

    private String eventId;
    private String warningType;
    private String origineTime;
    private String produceTime;
    private String eventTime;
    private String inElpId;
    private String inTypeId;
    private String inUid;
    private String inLabel;
    private String geohashString;
    private List<UFlag> flagList;

    @Override
    public String toString() {
        return "CongestWarningEvent{" +
                "eventId='" + eventId + '\'' +
                ", warningType='" + warningType + '\'' +
                ", origineTime='" + origineTime + '\'' +
                ", produceTime='" + produceTime + '\'' +
                ", eventTime='" + eventTime + '\'' +
                ", inElpId='" + inElpId + '\'' +
                ", inTypeId='" + inTypeId + '\'' +
                ", inUid='" + inUid + '\'' +
                ", inLabel='" + inLabel + '\'' +
                ", geohashString='" + geohashString + '\'' +
                ", flagList=" + flagList +
                '}';
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

    public String getOrigineTime() {
        return origineTime;
    }

    public void setOrigineTime(String origineTime) {
        this.origineTime = origineTime;
    }

    public String getProduceTime() {
        return produceTime;
    }

    public void setProduceTime(String produceTime) {
        this.produceTime = produceTime;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
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
