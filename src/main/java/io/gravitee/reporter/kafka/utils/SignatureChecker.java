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

import io.gravitee.gateway.api.Request;
import io.gravitee.reporter.kafka.enums.Channel;
import io.gravitee.reporter.kafka.model.FirstSigner;
import io.gravitee.reporter.kafka.model.SecondSigner;
import io.gravitee.reporter.kafka.model.Signable;
import io.micrometer.core.instrument.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignatureChecker {

    public static final int VERSION_OF_THE_OLD_ENCRYPTION = 505;
    private static final Logger LOGGER = LoggerFactory.getLogger(SignatureChecker.class);

    public boolean isNewEncryptMethod(String clientVersion) {
        int version = 0;
        if (StringUtils.isNotEmpty(clientVersion)) {
            clientVersion = clientVersion.replace(".", "");
            version = Integer.parseInt(clientVersion);
        }

        return version > 505;
    }

    public Boolean check(Signature signature, Request request, long timeout) throws Exception {
        if (!signature.checkIsIllegalArgument()) {
            throw new IllegalArgumentException("userId or appCode or paramsMap is null");
        } else {
            return this.checkAllChannel(signature, request, timeout);
        }
    }

    public String getChannel(Signature signature) throws Exception {
        if (!signature.checkIsIllegalArgument()) {
            throw new IllegalArgumentException("userId or appCode or paramsMap is null");
        } else {
            FirstSigner firstSigner = new FirstSigner();
            Signable signer = new SecondSigner(firstSigner);
            Channel[] var4 = Channel.values();
            int var5 = var4.length;
            for (int var6 = 0; var6 < var5; ++var6) {
                Channel c = var4[var6];
                signature.setChannelSecurityKey(c.getSecurityKey());
                try {
                    if (signature.getSignatureString().equals(signer.sign(signature))) {
                        return c.name();
                    }
                } catch (Exception var9) {
                    var9.printStackTrace();
                }
            }
            throw new IllegalArgumentException("Illlegal Channel");
        }
    }

    private Boolean checkAllChannel(Signature signature, Request request, long timeout) throws Exception {
        FirstSigner firstSigner = new FirstSigner();
        Signable signer = new SecondSigner(firstSigner);
        Channel[] var6 = Channel.values();
        int var7 = var6.length;
        for (Channel c : var6) {
            signature.setChannelSecurityKey(c.getSecurityKey());
            if (signature.getSignatureString().equals(signer.sign(signature))) {
                request.headers().add("accessChannel", c.getCnName());
                return Boolean.TRUE;
            }
        }
        throw new IllegalArgumentException("Illlegal Channel");
    }

}
