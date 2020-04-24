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
package io.gravitee.reporter.kafka.utils;

import org.springframework.util.StringUtils;

import java.util.Map;

public class Signature {

    private String signatureString;
    private String userId;
    private String appCode;
    private String clientVersion;
    private String accessChannel;
    private Map parameterMap;
    private String reqUrl;
    private String channelSecurityKey;

    public Signature() {
    }


    public String getTimeCode() throws Exception {
        return SecretUtil.decodeTime(this.getAppCode());
    }


    public Long getParamsTotalUniCode() {
        return SecretUtil.decodeASCII(this.parameterMap);
    }

    public String getUUID() throws Exception {
        return SecretUtil.decodeUUID(this.getAppCode());
    }

    public String getLocation() throws Exception {
        return SecretUtil.decodeLocation(this.getAppCode());
    }

    public Boolean checkIsIllegalArgument() {
        if (StringUtils.isEmpty(this.userId)) {
            return Boolean.FALSE;
        } else if (StringUtils.isEmpty(this.appCode)) {
            return Boolean.FALSE;
        } else {
            return this.isParamsEmptyBoolean() ? Boolean.FALSE : Boolean.TRUE;
        }
    }

    private boolean isParamsEmptyBoolean() {
        return this.getParameterMap() == null || this.getParameterMap().entrySet().size() == 0;
    }

    public String getSignatureString() {
        return this.signatureString;
    }

    public String getUserId() {
        return this.userId;
    }

    public String getAppCode() {
        return this.appCode;
    }

    public String getClientVersion() {
        return this.clientVersion;
    }

    public String getAccessChannel() {
        return this.accessChannel;
    }

    public Map getParameterMap() {
        return this.parameterMap;
    }

    public String getReqUrl() {
        return this.reqUrl;
    }

    public String getChannelSecurityKey() {
        return this.channelSecurityKey;
    }

    public void setSignatureString(String signatureString) {
        this.signatureString = signatureString;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    public void setClientVersion(String clientVersion) {
        this.clientVersion = clientVersion;
    }

    public void setAccessChannel(String accessChannel) {
        this.accessChannel = accessChannel;
    }

    public void setParameterMap(Map parameterMap) {
        this.parameterMap = parameterMap;
    }

    public void setReqUrl(String reqUrl) {
        this.reqUrl = reqUrl;
    }

    public void setChannelSecurityKey(String channelSecurityKey) {
        this.channelSecurityKey = channelSecurityKey;
    }
}
