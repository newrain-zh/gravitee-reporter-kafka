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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.gravitee.reporter.kafka.utils.AesUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class HostAddressTest {

    @Test
    public void shouldRaiseExceptionForNullHostList() {
        assertThatIllegalArgumentException().isThrownBy(() -> {
            HostAddress.stringifyHostAddresses(null);
        }).withMessage("Host Address argument must not be Null");
    }

    @Test
    public void shouldStringifySingleHostAddress() {
        List<HostAddress> hostAddressList = new ArrayList<HostAddress>();
        hostAddressList.add(new HostAddress("node1", 6062));
        String str = HostAddress.stringifyHostAddresses(hostAddressList);
        assertThat("node1:6062").isEqualTo(str);
    }

    @Test
    public void shouldStringifyHostAddressList() {
        List<HostAddress> hostAddressList = new ArrayList<HostAddress>();
        hostAddressList.add(new HostAddress("node1", 6062));
        hostAddressList.add(new HostAddress("node2", 6063));
        String str = HostAddress.stringifyHostAddresses(hostAddressList);
        assertThat("node1:6062,node2:6063").isEqualTo(str);
    }


    @Test
    public void test() {
        String s = "appcode=55115695dd40473655d53e251575f95edafeacc75bbe27985063df64d2ae723914e7c172bfac375ac0b1601570a00d38783da4f4a3f74434ebcba816cf995df6&clientVersion=5.0.2&deviceSystem=ios&deviceType=68F336FE-94B9-4232-BE69-AD438FAEA824&deviceCode=68F336FE-94B9-4232-BE69-AD438FAEA824&macAddress=02%3A00%3A00%3A00%3A00%3A00&sid=309488&sign=AAFF4F046E2C7CFA457EE1F34358FCF6&systemVersion=13.3.1&timestamp=1587611771520&userId=0&weHotelId=2d69d671dbff99895dcb7df7633ff801";
        String s1 = s.substring(s.indexOf("?") + 1, s.length() - 1);
        String[] split = s1.split("&");
        for (String str : split) {
            String key = str.substring(0, str.indexOf("="));
            String value = str.substring(str.indexOf("=") + 1);
            System.out.println("{" + key + ":" + value + "}");
        }
    }

    @Test
    public void testStr1(){
        String s = "appcode=55115695dd40473655d53e251575f95edafeacc75bbe27985063df64d2ae723914e7c172bfac375ac0b1601570a00d38783da4f4a3f74434ebcba816cf995df6&clientVersion=5.0.2&deviceSystem=ios&deviceType=68F336FE-94B9-4232-BE69-AD438FAEA824&deviceCode=68F336FE-94B9-4232-BE69-AD438FAEA824&macAddress=02%3A00%3A00%3A00%3A00%3A00&sid=309488&sign=AAFF4F046E2C7CFA457EE1F34358FCF6&systemVersion=13.3.1&timestamp=1587611771520&userId=0&weHotelId=2d69d671dbff99895dcb7df7633ff801";
        String[] split = s.split("&");
        for (String str : split) {
            String key = str.substring(0, str.indexOf("="));
            String value = str.substring(str.indexOf("=") + 1);
            System.out.println("{" + key + ":" + value + "}");
        }

    }

    @Test
    public void testStr() throws IOException {
        String str = "{\"body\":\"Hello\"}";
        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(str, Map.class);
        String decrypts = AesUtil.decrypts("2d69d671dbff99895dcb7df7633ff801");
        System.out.println(decrypts);
    }


}
