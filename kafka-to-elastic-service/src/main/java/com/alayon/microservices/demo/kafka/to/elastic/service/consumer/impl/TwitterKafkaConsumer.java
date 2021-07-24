package com.alayon.microservices.demo.kafka.to.elastic.service.consumer.impl;

import com.alayon.microservices.demo.config.KafkaConfigData;
import com.alayon.microservices.demo.kafka.admin.client.KafkaAdminClient;
import com.alayon.microservices.demo.kafka.to.elastic.service.consumer.KafkaConsumer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TwitterKafkaConsumer implements KafkaConsumer<Long, TwitterAvroModel> {

    private static final Logger log = LoggerFactory.getLogger(TwitterKafkaConsumer.class);

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final KafkaAdminClient kafkaAdminClient;

    private final KafkaConfigData kafkaConfigData;


    public TwitterKafkaConsumer(final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
                                final KafkaAdminClient kafkaAdminClient,
                                final KafkaConfigData kafkaConfigData) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConfigData = kafkaConfigData;
    }

    @Override
    @KafkaListener(id = "twitterTopicListener", topics = "${kafka-config.topic-name}")
    public void receive(@Payload final List<TwitterAvroModel> messages,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) final List<Integer> keys,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final List<Integer> partitions,
                        @Header(KafkaHeaders.OFFSET)final List<Integer> offsets)
    {
        log.info("{} number of message received with keys {}, partitions {} and offsets {}, " +
                 "Sending it to elastic: Thread id {}",
                messages.size(), keys.toString(), partitions.toString(), offsets.toString(), Thread.currentThread().getId());

    }
}
