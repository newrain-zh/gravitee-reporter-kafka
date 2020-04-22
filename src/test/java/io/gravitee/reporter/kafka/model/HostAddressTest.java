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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
        String s = "https://mobilegateway.bestwehotel.com/member_base/autoLogin.json?appcode=2e31af3adde71b501e1e698c9800222009fc6090e885054789b1326035aa6dde62aeb59f44b1c06d8eaf59ae81b1c700004f5ba192805300d78de9d4f2e562c3&clientVersion=5.0.2&deviceCode=68F336FE-94B9-4232-BE69-AD438FAEA824&deviceSystem=ios&anguage=ZH&os=iOS&sellerId=309488&sid=309488&sign=D6E4798500B91D52DC77C1BEEAFB5C26&systemVersion=13.3.1&timestamp=1587526734583&token=P9zo4ONa3MZhJzuNzRbjK8TXc3pxIbZuUyBGT8L2tsvIIDM%2BXDqy%2BNcdVLt%2BIGZZ&userId=0&weHotelId=0";
        String s1 = s.substring(s.indexOf("?") + 1, s.length() - 1);
        String[] split = s1.split("&");
        for (String str : split) {
            String key = str.substring(0, str.indexOf("="));
            String value = str.substring(str.indexOf("=") + 1);
            System.out.println("{" + key + ":" + value + "}");
        }


    }


}
