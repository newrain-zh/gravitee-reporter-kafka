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

import java.security.MessageDigest;

public class MD5Utils {

    private static final String[] hexDigits = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

    public MD5Utils() {
    }

    public static String generatePassword(String inputString) {
        return encodeByMD5(inputString);
    }

    public static String generateHexString(byte[] inputByte) {
        return encodeByMD5(inputByte);
    }

    private static String encodeByMD5(byte[] originByte) {
        if (originByte != null) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] results = md.digest(originByte);
                String resultString = byteArrayToHexString(results);
                return resultString.toUpperCase();
            } catch (Exception var5) {
                var5.printStackTrace();
            }
        }

        return null;
    }

    private static String encodeByMD5(String originString) {
        if (originString != null) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] results = md.digest(originString.getBytes());
                String resultString = byteArrayToHexString(results);
                String pass = resultString.toUpperCase();
                return pass;
            } catch (Exception var5) {
                var5.printStackTrace();
            }
        }

        return null;
    }

    private static String byteArrayToHexString(byte[] b) {
        StringBuffer resultSb = new StringBuffer();

        for (int i = 0; i < b.length; ++i) {
            resultSb.append(byteToHexString(b[i]));
        }

        return resultSb.toString();
    }

    private static String byteToHexString(byte b) {
        int n = b;
        if (b < 0) {
            n = 256 + b;
        }

        int d1 = n / 16;
        int d2 = n % 16;
        return hexDigits[d1] + hexDigits[d2];
    }
}
