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

import io.gravitee.reporter.kafka.utils.MD5Utils;
import io.gravitee.reporter.kafka.utils.Signature;

public class SecondSigner implements Signable {

    private Signable next;

    public SecondSigner(Signable next) {
        this.next = next;
    }

    @Override
    public String sign(Signature material, long timeout) throws Exception {
        return MD5Utils.generatePassword(next.sign(material, timeout) + material.getTimeCode(timeout) + material.getParamsTotalUniCode() + material.getUUID() + material.getLocation());
    }

}
