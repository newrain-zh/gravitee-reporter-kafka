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

import io.gravitee.reporter.kafka.config.KafkaConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SecretUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecretUtil.class);
    private static String key = "mobile_fkel_rrjk";

    public SecretUtil() {
    }

    public static boolean useNewEncrypt(String clientVersion) throws Exception {
        int version = 0;
        if (clientVersion != null) {
            clientVersion = clientVersion.replace(".", "");
            version = Integer.parseInt(clientVersion);
        }

        return version > 505;
    }

    public static String[] decodeAppCode(String appCode) throws Exception {
        String code = AesUtil.decrypts(appCode, key);
        String[] codeArray = code != null ? code.split("\\+") : new String[0];
        if (codeArray.length > 0) {
            return codeArray;
        } else {
            throw new Exception("");
        }
    }

    public static String decodeTime(String appCode, long timeout) throws Exception {
        String[] codeArray = decodeAppCode(appCode);
        if (codeArray.length > 0) {
            String timeCode = codeArray[0];
            if (timeOut(timeCode, timeout)) {
                throw new TimeoutException("time out");
            } else {
                return timeCode;
            }
        } else {
            return "";
        }
    }

    public static boolean timeOut(String timeCode, long timeout) {
        Date nowTime = new Date();
        if (Math.abs(nowTime.getTime() - Long.parseLong(timeCode)) < timeout) {
            return false;
        } else {
            LOGGER.error("时间超时,现在服务器时间为:{}({}) 客户端时间为(均为毫秒数):{}({}) 误差为:{}", nowTime.getTime(),
                    progressTime(String.valueOf(nowTime.getTime())), timeCode, progressTime(timeCode),
                    Math.abs(nowTime.getTime() - Long.parseLong(timeCode)));
            return true;
        }
    }


    // 将毫秒数转换为时间格式
    public static String progressTime(String progress) {
        long time = Long.parseLong(progress);
        Date date = new Date(time);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS", Locale.getDefault());
        return dateFormat.format(date);
    }


    public static String decodeLocation(String appCode) throws Exception {
        String[] codeArray = decodeAppCode(appCode);
        if (codeArray.length > 3) {
            String location = codeArray[3];
            return location;
        } else {
            return "";
        }
    }

    public static String decodeUUID(String appCode) throws Exception {
        String[] codeArray = decodeAppCode(appCode);
        if (codeArray.length > 2) {
            String uuid = codeArray[2];
            return uuid;
        } else {
            return "";
        }
    }

    public static long decodeASCII(Map map) {
        long ascII = 0L;

        for (Object obj : map.keySet()) {
            if (!"appcode".equals(obj) && !"sign".equals(obj)) {
                ascII += stringToAscII(map.get(obj));
            }
        }

        return ascII;
    }

    public static long stringToAscII(Object value) {
        long ascII = 0L;
        String[] v = (String[]) value;
        String s = String.valueOf(v[0]);
        char[] chars = s.toCharArray();
        for (char aChar : chars) {
            ascII += aChar;
        }

        return ascII;
    }
}
