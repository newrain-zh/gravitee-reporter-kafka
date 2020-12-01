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
        request.setMethod(HttpMethod.POST);
        request.setUri("/cli/app/frame/v1/jssdk/apiauthors?&deviceType=&sign=AB09945A1CB677753D0ACEBF51D7A6CD=&appcode=bbfcd9fc8dd10d77cb936faf202f05e1940fa2f3055380bfdaddac1861bd8fc2e4053e6e7dfdd1762a30be6e612cceda1acbf76715604f2f33f0cc182856e2ed&deviceCode=00000000-7720-6b4a-ffff-ffffacfd0acc&clientVersion=5.1.1&userId=0&systemVersion=10&sid=306265");
        request.setBody("pageSize=20&pageNum&");

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
        messageConsumed();
//        await().until(messageConsumed(), equalTo(2));
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
        consumer.subscribe("my-kafka-topic", ar -> {
            if (ar.succeeded()) {
                LOGGER.info("subscribed");
            } else {
                LOGGER.info("Could not subscribe " + ar.cause().getMessage());
            }
        });
        return logs::size;
    }


    @Test
    public void testGetParams() {
        String params = getParams("api/v1?appcode=1");
        System.out.println(params);
        String s = "application/json;charset=UTF-8";
        System.out.println(s.contains("application/json"));
    }

    @Test
    public void testString() {
        String s = "pageNum";
        String key = s.substring(0, s.indexOf("="));
        String value = s.substring(s.indexOf("=") + 1);
        System.out.println(key);
        System.out.println(value);
    }


    private String getParams(String url) {
        if (!url.contains("?")) {
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
        LOGGER.info("url:{}", url);
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
