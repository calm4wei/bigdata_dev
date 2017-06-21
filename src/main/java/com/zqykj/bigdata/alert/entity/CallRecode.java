package com.zqykj.bigdata.alert.entity;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * Created by weifeng on 2017/5/26.
 */
public class CallRecode implements Serializable {

    /**
     * 本段号码
     */
    private String callNumber;
    private String callNumberLoc;
    private String callIMSI;
    private String callIMEI;

    /**
     * 对端号码
     */
    private String receiverNumber;
    private String receiverNumberLoc;
    private String receiverIMSI;
    private String receiverIMEI;

    /**
     * 呼叫标识
     */
    private String callId;
    private String callDate;
    private String callTime;

    /**
     * 通话时长
     */
    private Integer callDuration;
    private String lac;
    private String ci;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public String getCallNumber() {
        return callNumber;
    }

    public void setCallNumber(String callNumber) {
        this.callNumber = callNumber;
    }

    public String getCallNumberLoc() {
        return callNumberLoc;
    }

    public void setCallNumberLoc(String callNumberLoc) {
        this.callNumberLoc = callNumberLoc;
    }

    public String getCallIMSI() {
        return callIMSI;
    }

    public void setCallIMSI(String callIMSI) {
        this.callIMSI = callIMSI;
    }

    public String getCallIMEI() {
        return callIMEI;
    }

    public void setCallIMEI(String callIMEI) {
        this.callIMEI = callIMEI;
    }

    public String getReceiverNumber() {
        return receiverNumber;
    }

    public void setReceiverNumber(String receiverNumber) {
        this.receiverNumber = receiverNumber;
    }

    public String getReceiverNumberLoc() {
        return receiverNumberLoc;
    }

    public void setReceiverNumberLoc(String receiverNumberLoc) {
        this.receiverNumberLoc = receiverNumberLoc;
    }

    public String getReceiverIMSI() {
        return receiverIMSI;
    }

    public void setReceiverIMSI(String receiverIMSI) {
        this.receiverIMSI = receiverIMSI;
    }

    public String getReceiverIMEI() {
        return receiverIMEI;
    }

    public void setReceiverIMEI(String receiverIMEI) {
        this.receiverIMEI = receiverIMEI;
    }

    public String getCallId() {
        return callId;
    }

    public void setCallId(String callId) {
        this.callId = callId;
    }

    public String getCallDate() {
        return callDate;
    }

    public void setCallDate(String callDate) {
        this.callDate = callDate;
    }

    public String getCallTime() {
        return callTime;
    }

    public void setCallTime(String callTime) {
        this.callTime = callTime;
    }

    public Integer getCallDuration() {
        return callDuration;
    }

    public void setCallDuration(Integer callDuration) {
        this.callDuration = callDuration;
    }

    public String getLac() {
        return lac;
    }

    public void setLac(String lac) {
        this.lac = lac;
    }

    public String getCi() {
        return ci;
    }

    public void setCi(String ci) {
        this.ci = ci;
    }
}
