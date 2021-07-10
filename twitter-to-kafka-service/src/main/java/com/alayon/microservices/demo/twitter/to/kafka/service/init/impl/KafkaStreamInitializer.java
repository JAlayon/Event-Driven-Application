package com.alayon.microservices.demo.twitter.to.kafka.service.init.impl;

import com.alayon.microservices.demo.config.KafkaConfigData;
import com.alayon.microservices.demo.kafka.admin.client.KafkaAdminClient;
import com.alayon.microservices.demo.twitter.to.kafka.service.init.StreamInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializer implements StreamInitializer {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamInitializer.class);
    private final KafkaConfigData kafkaConfigData;
    private final KafkaAdminClient kafkaAdminClient;

    public KafkaStreamInitializer(final KafkaConfigData kafkaConfigData,
                                  final KafkaAdminClient kafkaAdminClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaAdminClient = kafkaAdminClient;
    }

    @Override
    public void init() {
        kafkaAdminClient.createTopics();
        kafkaAdminClient.checkSchemaRegistry();
        log.info("Topic(s) with name {} are ready for operations!", kafkaConfigData.getTopicNamesToCreate().toArray());
    }
}
