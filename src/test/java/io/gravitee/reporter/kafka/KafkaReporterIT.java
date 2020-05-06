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

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        request.setUri("http://172.21.221.62:8082/demo-api/test.json?weHotelId&userId=0&latlng=31.244351042232392,121.50303128227807&clientVersion=5.0.3&deviceType=CLT-AL00&deviceCode=00000000-1940-9668-0000-000042d74f33&systemVersion=29&sellerId=415153&sid=415153&umChannel=415153&os=android&deviceSystem=android&deviceId=00000000-1940-9668-0000-000042d74f33&sign=1EB52E5AB4F3A8C22ABFBB4C807217BD&appcode=7c2fc52429d54efc88764946203870d79ce1c8510c1725c697a5eeca0bb71db9d2f254afb13fcb9385d12a827991476d75a16dbe75da34ba0cce741f602d1ec213f21a78c9ae90b63f45e649790bbc712add79393add8aaf39e29bbd5212ce42");
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
}
