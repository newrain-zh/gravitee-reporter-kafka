/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.reporter.kafka.model;

import org.springframework.beans.BeanUtils;

import java.sql.Timestamp;

/**
 * Created by pan.wu on 2018/10/29.
 */
public class GatewayLoggerMsgReq extends GatewayLoggerData {
    private String message;
    private String loggerLevel;
    private Timestamp timestamp;
    private String env;

    /**
     * @param loggerLevel 日志级别
     * @param message     消息内容
     * @param env         环境
     */
    public GatewayLoggerMsgReq(String loggerLevel, String message, String env) {
        this.setMessage(message);
        this.setLoggerLevel(loggerLevel);
        this.setTimestamp(new Timestamp(System.currentTimeMillis()));
        this.setEnv(env);
    }

    public GatewayLoggerMsgReq(String loggerLevel, String env, GatewayLoggerData data) {
        this.setLoggerLevel(loggerLevel);
        this.setTimestamp(new Timestamp(System.currentTimeMillis()));
        this.setEnv(env);

        BeanUtils.copyProperties(data, this);
    }

    public GatewayLoggerMsgReq(String loggerLevel, String message, String env, GatewayLoggerData data) {
        this.setMessage(message);
        this.setLoggerLevel(loggerLevel);
        this.setTimestamp(new Timestamp(System.currentTimeMillis()));
        this.setEnv(env);

        BeanUtils.copyProperties(data, this);
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
