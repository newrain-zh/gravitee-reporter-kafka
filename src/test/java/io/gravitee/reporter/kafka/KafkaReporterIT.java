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
import io.gravitee.common.http.HttpHeaders;
import io.gravitee.common.http.HttpMethod;
import io.gravitee.reporter.api.common.Request;
import io.gravitee.reporter.api.common.Response;
import io.gravitee.reporter.api.log.Log;
import io.gravitee.reporter.kafka.config.KafkaConfiguration;
import io.gravitee.reporter.kafka.model.HostAddress;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.serialization.JsonObjectDeserializer;
import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfigImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {ContextTestConfiguration.class})
public class KafkaReporterIT {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaReporterIT.class);

    @Inject
    private KafkaConfiguration kafkaConfiguration;

    @Inject
    private KafkaReporter reporter;

    @Inject
    private Vertx vertx;

    public static EmbeddedKafkaConfigImpl conf = new EmbeddedKafkaConfigImpl(6001, 6000,
            new scala.collection.immutable.HashMap<String, String>(),
            new scala.collection.immutable.HashMap<String, String>(),
            new scala.collection.immutable.HashMap<String, String>());

    public static EmbeddedKafka$ kafkaUnitServer = EmbeddedKafka$.MODULE$;

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException {
        // kafkaUnitServer.startup();
        File graviteeConf = ResourceUtils.getFile("classpath:gravitee-embedded.yml");
        System.setProperty("gravitee.conf", graviteeConf.getAbsolutePath());

        kafkaUnitServer.start(conf);
    }

    @AfterClass
    public static void after() {
        kafkaUnitServer.stop();
    }

    @Test
    public void shouldCreateInstanceAndSetEnvironnement() throws Exception {

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/x-www-form-urlencoded;charset=utf8");

        Request request = new Request();
        request.setHeaders(headers);
        request.setMethod(HttpMethod.GET);
        request.setUri("http://172.21.221.62:8082/cli/app/frame/v1/fdetail?appcode=05a70406a1822562b2fa979bf707c0bdd78aaf3c100ec4684c0fdfb36682ffbef206535807908e01d5ab7720f40a9eb704419d845c559ed4b6c90b7aa3213692&clientVersion=5.0.3&deviceCode=CE46DC1C-30D3-48E9-9B08-BAD1C00FCA68&deviceSystem=ios&deviceType=iPhone9%2C1&frmCode=feed_slide&language=ZH&macAddress=02%3A00%3A00%3A00%3A00%3A00&os=iOS&page=6&pageSize=20&sid=415153&sign=BD09CFA639990552C9E26AE05165E311&systemVersion=13.3.1&timestamp=1589178583588&userId=0&weHotelId=0");
        request.setBody("");

        Response response = new Response();
        response.setStatus(201);
        response.setHeaders(headers);
        response.setBody("test ok");

        Log log = new Log(System.currentTimeMillis());
        log.setRequestId("1");
        log.setClientRequest(request);
        log.setClientResponse(response);

        reporter.report(log);
        log.setRequestId("2");

        reporter.report(log);

        await().until(messageConsumed(), equalTo(2));
    }

    private Callable<Integer> messageConsumed() {
        Map<String, String> configConsumer = new HashMap<>();
        configConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HostAddress.stringifyHostAddresses(kafkaConfiguration.getHostsAddresses()));
        configConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonObjectDeserializer.class.getCanonicalName());
        configConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonObjectDeserializer.class.getCanonicalName());
        configConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "topic");
        configConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, JsonObject> consumer = KafkaConsumer.create(Vertx.vertx(), configConsumer);
        List<JsonObject> logs = new ArrayList<>();
        consumer.handler(recorded -> {
            logs.add(recorded.value());
            LOGGER.info("Processing key=" + recorded.key() + ",value=" + recorded.value() + ",partition=" + recorded.partition() + ",offset=" + recorded.offset());
        });
        consumer.subscribe("topic", ar -> {
            if (ar.succeeded()) {
                LOGGER.info("subscribed");
            } else {
                LOGGER.info("Could not subscribe " + ar.cause().getMessage());
            }
        });
        return () -> logs.size();
    }


    @Test
    public void testGetParams(){
        String params = getParams("api/v1?appcode=1");
        System.out.println(params);
        String s = "application/json;charset=UTF-8";
        System.out.println(s.contains("application/json"));
    }


    private String getParams(String url) {
        if (!url.contains("?")){
            return "";
        }
        System.out.println("url:" + url);
        ObjectMapper objectMapper = new ObjectMapper();
        String s1 = url.substring(url.indexOf("?") + 1);
        String[] split = s1.split("&");
        if (split.length == 0) {
            return "";
        }
        Map<String, String> resultMap = new HashMap<>(split.length);
        LOGGER.info("url:{}",url);
        LOGGER.info("split:{}", Arrays.toString(split));
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
