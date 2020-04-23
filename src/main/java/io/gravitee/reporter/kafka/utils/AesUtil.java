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

import io.micrometer.core.instrument.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class AesUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(AesUtil.class);
    private static String key = "mobile_team_pkey";

    public AesUtil() {
    }

    public static String decrypts(String source) {
        if (StringUtils.isBlank(source)) {
            LOGGER.error("解密异常,输入的待解密参数为空");
            return null;
        } else if ("0".equals(source)) {
            return null;
        } else {
            try {
                byte[] raw = key.getBytes("ASCII");
                SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
                Cipher cipher = Cipher.getInstance("AES");
                cipher.init(2, skeySpec);
                byte[] encrypted1 = hex2byte(source);
                byte[] original = cipher.doFinal(encrypted1);
                return new String(original);
            } catch (Exception var6) {
                LOGGER.error("解密异常,输入的待解密参数为:" + source);
                return null;
            }
        }
    }

    public static String decrypts(String source, String clientKey) {
        if (StringUtils.isBlank(source)) {
            LOGGER.error("解密异常,输入的待解密参数为空");
            return null;
        } else if ("0".equals(source)) {
            return null;
        } else {
            try {
                byte[] raw = clientKey.getBytes("ASCII");
                SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
                Cipher cipher = Cipher.getInstance("AES");
                cipher.init(2, skeySpec);
                byte[] encrypted1 = hex2byte(source);
                byte[] original = cipher.doFinal(encrypted1);
                return new String(original);
            } catch (Exception var7) {
                LOGGER.error("解密异常,输入的待解密参数为:" + source);
                return null;
            }
        }
    }

    public static String encrypt(String source) {
        if (StringUtils.isBlank(source)) {
            throw new RuntimeException("加密异常,输入待加密参数为空");
        } else {
            try {
                byte[] raw = key.getBytes("ASCII");
                SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
                Cipher cipher = Cipher.getInstance("AES");
                cipher.init(1, skeySpec);
                byte[] encrypted = cipher.doFinal(source.getBytes());
                return byte2hex(encrypted).toLowerCase();
            } catch (Exception var5) {
                throw new RuntimeException("加密异常,输入待加密参数为空");
            }
        }
    }

    private static byte[] hex2byte(String sourceString) {
        if (sourceString == null) {
            return null;
        } else {
            int l = sourceString.length();
            if (l % 2 == 1) {
                return null;
            } else {
                byte[] b = new byte[l / 2];

                for (int i = 0; i != l / 2; ++i) {
                    b[i] = (byte) Integer.parseInt(sourceString.substring(i * 2, i * 2 + 2), 16);
                }

                return b;
            }
        }
    }

    private static String byte2hex(byte[] sourceByteArray) {
        StringBuilder result = new StringBuilder();

        for (int n = 0; n < sourceByteArray.length; ++n) {
            String temp = Integer.toHexString(sourceByteArray[n] & 255);
            if (temp.length() == 1) {
                result.append("0" + temp);
            } else {
                result.append(temp);
            }
        }

        return result.toString().toUpperCase();
    }

    public static void main(String[] args) {
        String decrypts = AesUtil.decrypts("2d69d671dbff99895dcb7df7633ff801");
        System.out.println(decrypts);
    }
}
