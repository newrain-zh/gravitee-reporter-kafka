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
import io.gravitee.reporter.kafka.model.GatewayLoggerMsgReq;
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
import java.net.URLDecoder;
import java.sql.Timestamp;
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
        if (kafkaProducer != null) {
            if (reportable instanceof Log) {
                Log log = (Log) reportable;
                Request clientRequest = log.getClientRequest();
                Response clientResponse = log.getClientResponse();
                GatewayLoggerMsgReq gatewayLoggerData = new GatewayLoggerMsgReq("info", "", "uat");
                HttpMethod method = clientRequest.getMethod();
                gatewayLoggerData.setRequestIp(getIp(clientRequest));
                gatewayLoggerData.setRequestMethod(clientRequest.getMethod().name());
                gatewayLoggerData.setTraceId(clientRequest.getHeaders().getFirst("X-Gravitee-Transaction-Id"));
                gatewayLoggerData.setRequestUrl(URLDecoder.decode(clientRequest.getUri()));
                ObjectMapper mapper = new ObjectMapper();
                String requestParams = "";
                if ("GET".equals(method.name())) {
                    requestParams = getParams(URLDecoder.decode(clientRequest.getUri()));
                    if (!StringUtils.isEmpty(requestParams)) {
                        gatewayLoggerData.setRequstData(requestParams);
                    }
                } else if ("POST".equals(method.name())) {
                    String contentType = clientRequest.getHeaders().getFirst("Content-Type");
                    //表单请求
                    if (contentType.contains("application/x-www-form-urlencoded")) {
                        if (!StringUtils.isEmpty(clientRequest.getBody())) {
                            requestParams = getParamsFromContentType(URLDecoder.decode(clientRequest.getBody()));
                        }
                    } else if (contentType.contains("application/json")) {
                        requestParams = clientRequest.getBody();
                    }
                    gatewayLoggerData.setRequstData(clientRequest.getBody());
                }
                //获取mac address and accessChannel
                Map<String, Object> map1 = new HashMap(0);
                try {
                    if (!StringUtils.isEmpty(requestParams)) {
                        map1 = mapper.readValue(requestParams, Map.class);
                    }
                    String macAddress = "";
                    if (!StringUtils.isEmpty(map1.get("macAddress"))) {
                        macAddress = URLDecoder.decode(String.valueOf(map1.get("macAddress")));
                    } else {
                        macAddress = clientRequest.getHeaders().getFirst("macAddress");
                    }
                    gatewayLoggerData.setMacAddress(macAddress);
//                    Map accessChannelData = getAccessChannelData(clientRequest, map1);
//                    String accessChannel = getAccessChannel(accessChannelData, map1);
//                    LOGGER.info("accessChannel:{}", clientRequest.getHeaders().getFirst("accessChannel"));
                    gatewayLoggerData.setAccessChannel(clientRequest.getHeaders().getFirst("accessChannel"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String weHotelId = getWehotelId(clientRequest, clientResponse, String.valueOf(map1.get("weHotelId")));
                gatewayLoggerData.setWeHotelId(weHotelId);
                //相应数据处理
                gatewayLoggerData.setResponseCode(String.valueOf(clientResponse.getStatus()));
                gatewayLoggerData.setUsedTimeMS(clientResponse.getHeaders().getFirst("response-time"));
                gatewayLoggerData.setResponseData(clientResponse.getBody());
                gatewayLoggerData.setEnv(kafkaConfiguration.getEnv());
                gatewayLoggerData.setTimestamp(new Timestamp(System.currentTimeMillis()));
                gatewayLoggerData.setLoggerLevel("info");
                gatewayLoggerData.setMessage("");
                if (kafkaProducer != null) {
                    KafkaProducerRecord<String, JsonObject> record = KafkaProducerRecord.create(kafkaConfiguration.getKafkaTopic(), JsonObject.mapFrom(gatewayLoggerData));
                    kafkaProducer.write(record, done -> {
                        String message;
                        if (done.succeeded()) {
                            RecordMetadata recordMetadata = done.result();
                            message = String.format("Topic=%s partition=%s offset=%s message %s",
                                    record.value(),
                                    recordMetadata.getTopic(),
                                    recordMetadata.getPartition(),
                                    recordMetadata.getOffset());
                        } else {
                            message = String.format("Message %s not written on topic=%s", record.value(), kafkaConfiguration.getKafkaTopic());
                        }
//                        LOGGER.info(message);
                    });
                }
            }

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


    private String getAccessChannel(Map<String, Object> params, Map<String, Object> all) {
        Signature signature = new Signature();
        signature.setParameterMap(all);
        signature.setAppCode(String.valueOf(params.get("appcode")));
        signature.setSignatureString(String.valueOf(params.get("sign")));
        signature.setUserId(String.valueOf(params.get("userId")));
        signature.setClientVersion(String.valueOf(params.get("clientVersion")));
        try {
            return SignatureChecker.getChannel(signature);
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
        if (!url.contains("?")) {
            return "";
        }
//        LOGGER.info("url:{}", url);
        ObjectMapper objectMapper = new ObjectMapper();
        String s1 = url.substring(url.indexOf("?") + 1);
        String[] split = s1.split("&");
        if (split.length == 0) {
            return "";
        }
        Map<String, String> resultMap = new HashMap<>(split.length);
        for (String s : split) {
            if (!StringUtils.isEmpty(s)) {
                String key = s.substring(0, s.indexOf("="));
                String value = s.substring(s.indexOf("=") + 1);
                resultMap.put(key, value);
            }
        }
        try {
            return objectMapper.writeValueAsString(resultMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 表单数据处理
     *
     * @param body
     * @return
     */
    private String getParamsFromContentType(String body) {
        if (StringUtils.isEmpty(body)) {
            return "";
        }
        ObjectMapper objectMapper = new ObjectMapper();
        String[] split = body.split("&");
//        LOGGER.info("body:{}", body);
        Map<String, String> resultMap = new HashMap<>(split.length);
        for (String s : split) {
            if (!StringUtils.isEmpty(s)) {
                if (s.contains("=")) {
                    String key = s.substring(0, s.indexOf("="));
                    String value = s.substring(s.indexOf("=") + 1);
                    resultMap.put(key, value);
                } else {
                    resultMap.put(s, "");
                }
            }
        }
        try {
            return objectMapper.writeValueAsString(resultMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public final static boolean isJSONValid2(String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private String getIp(Request request) {
        String first = request.getHeaders().getFirst("x-forwarded-for");
        if (!StringUtils.isEmpty(first)) {
            return first;
        }
        first = request.getHeaders().getFirst("x-real-ip");
        if (!StringUtils.isEmpty(first)) {
            return first;
        }
        first = request.getHeaders().getFirst("x-user");
        if (!StringUtils.isEmpty(first)) {
            return first;
        }
        return "";
    }

    /**
     * 从header or 参数里面获取数据
     *
     * @param request
     * @param parmas
     * @return
     */
    private Map getAccessChannelData(Request request, Map parmas) {
        String appcode = request.getHeaders().getFirst("appcode");
        if (StringUtils.isEmpty(appcode)) {
            appcode = String.valueOf(parmas.get("appcode"));
        }
        String sign = request.getHeaders().getFirst("sign");
        if (StringUtils.isEmpty(sign)) {
            sign = String.valueOf(parmas.get("sign"));
        }
        String userId = request.getHeaders().getFirst("userId");
        if (StringUtils.isEmpty(userId)) {
            userId = String.valueOf(parmas.get("userId"));
        }
        String clientVersion = request.getHeaders().getFirst("clientVersion");
        if (StringUtils.isEmpty(clientVersion)) {
            clientVersion = String.valueOf(parmas.get("clientVersion"));
        }
        Map<String, String> map = new HashMap<>();
        map.put("appcode", appcode);
        map.put("sign", sign);
        map.put("userId", userId);
        map.put("clientVersion", clientVersion);
        return map;
    }

    private String getWehotelId(Request request, Response response, String weHotelId) {
        if (StringUtils.isEmpty(weHotelId)) {
            return "0";
        }
        weHotelId = request.getHeaders().getFirst("weHotelId");
        if (!StringUtils.isEmpty(weHotelId)) {
            return weHotelId;
        }
        weHotelId = response.getHeaders().getFirst("mid");
        if (!StringUtils.isEmpty(weHotelId)) {
            return weHotelId;
        }
        return AesUtil.decrypts(weHotelId);
    }

}
