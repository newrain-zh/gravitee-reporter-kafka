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
package io.gravitee.reporter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.gravitee.common.http.HttpMethod;
import io.gravitee.common.service.AbstractService;
import io.gravitee.reporter.api.Reportable;
import io.gravitee.reporter.api.Reporter;
import io.gravitee.reporter.api.common.Request;
import io.gravitee.reporter.api.common.Response;
import io.gravitee.reporter.api.health.EndpointStatus;
import io.gravitee.reporter.api.http.Metrics;
import io.gravitee.reporter.api.log.Log;
import io.gravitee.reporter.api.monitor.Monitor;
import io.gravitee.reporter.kafka.config.KafkaConfiguration;
import io.gravitee.reporter.kafka.model.GatewayLoggerData;
import io.gravitee.reporter.kafka.model.MessageType;
import io.gravitee.reporter.kafka.utils.AesUtil;
import io.gravitee.reporter.kafka.utils.Signature;
import io.gravitee.reporter.kafka.utils.SignatureChecker;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class KafkaReporter extends AbstractService implements Reporter {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaReporter.class);

    @Autowired(required = false)
    private KafkaProducer<String, JsonObject> kafkaProducer;

    @Autowired(required = false)
    private KafkaConfiguration kafkaConfiguration;

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        if (kafkaProducer != null) {
            kafkaProducer.close(res -> {
                if (res.succeeded()) {
                    LOGGER.info("Kafka producer closed");
                } else {
                    LOGGER.error("Fail to close Kafka producer");
                }
            });
        }
    }

    @Override
    public void report(Reportable reportable) {
        LOGGER.info("reportable start1");
        if (kafkaProducer != null) {
            Log log = (Log) reportable;
            Request clientRequest = log.getClientRequest();
            Response clientResponse = log.getClientResponse();
            GatewayLoggerData gatewayLoggerData = new GatewayLoggerData();
            HttpMethod method = clientRequest.getMethod();
            gatewayLoggerData.setRequestUrl(clientRequest.getUri());
            gatewayLoggerData.setRequestIp(clientRequest.getHeaders().getFirst("x-user-ip"));
            gatewayLoggerData.setRequestMethod(clientRequest.getMethod().name());
            gatewayLoggerData.setTraceId(clientRequest.getHeaders().getFirst("X-Gravitee-Transaction-Id"));
            ObjectMapper mapper = new ObjectMapper();
            String requestParams = "";
            LOGGER.info("reportable start2");
            if ("GET".equals(method.name())) {
                requestParams = getParams(clientRequest.getUri());
                gatewayLoggerData.setRequstData(requestParams);
            } else if ("POST".equals(method.name())) {
                requestParams = clientRequest.getBody();
                gatewayLoggerData.setRequstData(clientRequest.getBody());
            }
            LOGGER.info("reportable start3");
            //获取mac address and accessChannel
            try {
                Map<String, Object> map1 = mapper.readValue(requestParams, Map.class);
                gatewayLoggerData.setMacAddress(String.valueOf(map1.get("macAddress")));
                String accessChannel = getAccessChannel(map1, 30000);
                if (StringUtils.isEmpty(map1.get("wehotelId"))) {
                    gatewayLoggerData.setWeHotelId("0");
                } else {
                    gatewayLoggerData.setWeHotelId(AesUtil.decrypts(String.valueOf(map1.get("wehotelId"))));
                }
                gatewayLoggerData.setAccessChannel(accessChannel);
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOGGER.info("reportable start4");
            //相应数据处理
            String body = clientResponse.getBody();
            Map<String, String> responseMap = new HashMap<>();
            try {
                Map<String, Object> map = mapper.readValue(body, Map.class);
                if (map.containsKey("code")) {
                    responseMap.put("code", String.valueOf(map.get("code")));
                } else if (map.containsKey("msgCode")) {
                    responseMap.put("code", String.valueOf(map.get("msgCode")));
                }
                if (map.containsKey("msg")) {
                    responseMap.put("msg", String.valueOf(map.get("msg")));
                } else if (map.containsKey("message")) {
                    responseMap.put("msg", String.valueOf(map.get("message")));
                }
                gatewayLoggerData.setResponseCode(mapper.writeValueAsString(responseMap));
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOGGER.info("reportable start5");
            KafkaProducerRecord<String, JsonObject> record = KafkaProducerRecord.create(kafkaConfiguration.getKafkaTopic(), JsonObject.mapFrom(gatewayLoggerData));
            kafkaProducer.write(record, done -> {
                String message;
                if (done.succeeded()) {
                    LOGGER.info("reportable start6");
                    RecordMetadata recordMetadata = done.result();
                    message = String.format("Topic=%s partition=%s offset=%s message %s",
                            record.value(),
                            recordMetadata.getTopic(),
                            recordMetadata.getPartition(),
                            recordMetadata.getOffset());
                } else {
                    message = String.format("Message %s not written on topic=%s", record.value(), kafkaConfiguration.getKafkaTopic());
                }
                LOGGER.info(message);
            });
        }
    }

    @Override
    public boolean canHandle(Reportable reportable) {
        if (kafkaConfiguration != null) {
            MessageType messageType;
            if (kafkaConfiguration.getMessageTypes().isEmpty()) {
                return true;
            } else if (reportable instanceof Metrics) {
                messageType = MessageType.REQUEST;
            } else if (reportable instanceof EndpointStatus) {
                messageType = MessageType.HEALTH;
            } else if (reportable instanceof Monitor) {
                messageType = MessageType.MONITOR;
            } else if (reportable instanceof Log) {
                messageType = MessageType.LOG;
            } else {
                return false;
            }
            return kafkaConfiguration.getMessageTypes().contains(messageType);
        }
        return false;
    }


    private String getAccessChannel(Map<String, Object> params, long timeout) {
        Signature signature = new Signature();
        signature.setParameterMap(params);
        signature.setAppCode(String.valueOf(params.get("appcode")));
        signature.setSignatureString(String.valueOf(params.get("sign")));
        signature.setUserId(String.valueOf(params.get("userId")));
        signature.setClientVersion(String.valueOf(params.get("clientVersion")));
        try {
            return new SignatureChecker().getChannel(signature, timeout);
        } catch (Exception e) {
            return "";
        }
    }


    /**
     * Get请求中获取参数
     *
     * @param url
     */
    private String getParams(String url) {
        ObjectMapper objectMapper = new ObjectMapper();
        String s1 = url.substring(url.indexOf("?") + 1, url.length() - 1);
        String[] split = s1.split("&");
        Map<String, String> resultMap = new HashMap<>(split.length);
        for (String s : split) {
            String key = s.substring(0, s.indexOf("="));
            String value = s.substring(s.indexOf("=") + 1);
            resultMap.put(key, value);
        }
        try {
            return objectMapper.writeValueAsString(resultMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;

    }
}
