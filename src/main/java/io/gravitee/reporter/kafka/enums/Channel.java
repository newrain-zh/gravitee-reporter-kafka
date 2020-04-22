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
package io.gravitee.reporter.kafka.enums;

public enum Channel {

    JJ_INTEGRATION_IOS("锦江旅行IOS", "jklfjioqu4io23jio4p27f890asuf9082390jrio7c94ce95tt"),

    JJ_INTEGRATION_ANDROID("锦江旅行ANDROID", "vadjlr4k3o;qj4io23ug9034uji5rjn34io5u83490u590jvi0"),

    BOTAO_APP_IOS("铂涛App_IOS", "jf94nf4ijcnefk3dcdlmvklfjvkrfmdkvmfdvr334pcir"),

    BOTAO_APP_ANDROID("铂涛App_Android", "94r4rjjof023edendwekndaosdmascjwecr123sdqsacnr"),

    BOTAO_APP_ANDROID_2("铂涛App_Android", "vadjlr4k3o;qj4io23ug9034uji5rjn34io5u83490u5903huq"),

    WEHOTEL_INTERNATIONAL_IOS("wehotel_app英文版IOS", "6os5p44i0a23wjej5;hjio4p27f89suf58ed82390w984"),

    WEHOTEL_INTERNATIONAL_ANDROID("wehotel_app英文版ANDROID", "ld45fe2r4ff54lnk3zx96o;e4gfh6jhx90dq76zdc3v2c");

    private String cnName;
    private String securityKey;

    private Channel(String cnName, String securityKey) {
        this.cnName = cnName;
        this.securityKey = securityKey;
    }

    public String getCnName() {
        return this.cnName;
    }

    public String getSecurityKey() {
        return this.securityKey;
    }

}
