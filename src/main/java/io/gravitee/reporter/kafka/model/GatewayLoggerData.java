/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gravitee.reporter.kafka.model;

import java.sql.Timestamp;

/**
 * 网关日志打印使用，监控所有请求
 * Created by pan.wu on 2018/10/29.
 */

public class GatewayLoggerData {
    private String requestUrl;//请求地址
    private String requestIp;//请求ip
    private String requestMethod;//请求类型
    private String requstData;//请求入参(json字符串，客户端设备信息等并入该字段)
    private String responseCode;//响应code
    private String responseData;//响应报文，避免报文太大，只返回code、message，例如{"code":0,"msg":"成功"}
    private String usedTimeMS;//耗时，单位ms
    private String macAddress;//物理地址
    private String accessChannel;//客户端渠道，如锦江android
    private String weHotelId = "0";//会员id
    private String traceId;

    private String message;
    private String loggerLevel = "";
    private Timestamp timestamp;
    private String env;

    public String getRequestUrl() {
        return requestUrl;
    }

    public void setRequestUrl(String requestUrl) {
        this.requestUrl = requestUrl;
    }

    public String getRequestIp() {
        return requestIp;
    }

    public void setRequestIp(String requestIp) {
        this.requestIp = requestIp;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public void setRequestMethod(String requestMethod) {
        this.requestMethod = requestMethod;
    }

    public String getRequstData() {
        return requstData;
    }

    public void setRequstData(String requstData) {
        this.requstData = requstData;
    }

    public String getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(String responseCode) {
        this.responseCode = responseCode;
    }

    public String getResponseData() {
        return responseData;
    }

    public void setResponseData(String responseData) {
        this.responseData = responseData;
    }

    public String getUsedTimeMS() {
        return usedTimeMS;
    }

    public void setUsedTimeMS(String usedTimeMS) {
        this.usedTimeMS = usedTimeMS;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }

    public String getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(String accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getWeHotelId() {
        return weHotelId;
    }

    public void setWeHotelId(String weHotelId) {
        this.weHotelId = weHotelId;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getLoggerLevel() {
        return loggerLevel;
    }

    public void setLoggerLevel(String loggerLevel) {
        this.loggerLevel = loggerLevel;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }
}
