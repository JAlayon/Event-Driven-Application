package com.alayon.microservices.demo.kafka.to.elastic.service.consumer.impl;

import com.alayon.microservices.demo.config.KafkaConfigData;
import com.alayon.microservices.demo.elastic.index.client.service.ElasticIndexClient;
import com.alayon.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.alayon.microservices.demo.kafka.admin.client.KafkaAdminClient;
import com.alayon.microservices.demo.kafka.to.elastic.service.consumer.KafkaConsumer;
import com.alayon.microservices.demo.kafka.to.elastic.service.transformer.AvroToElasticModelTransformer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
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

    private final AvroToElasticModelTransformer avroToElasticModelTransformer;

    private final ElasticIndexClient<TwitterIndexModel> elasticIndexClient;

    public TwitterKafkaConsumer(final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
                                final KafkaAdminClient kafkaAdminClient,
                                final KafkaConfigData kafkaConfigData,
                                final AvroToElasticModelTransformer avroToElasticModelTransformer,
                                final ElasticIndexClient<TwitterIndexModel> elasticIndexClient) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConfigData = kafkaConfigData;
        this.avroToElasticModelTransformer = avroToElasticModelTransformer;
        this.elasticIndexClient = elasticIndexClient;
    }

    @EventListener
    public void onAppStarted(ApplicationStartedEvent event){
        kafkaAdminClient.checkTopicsCreated();
        log.info("Topic(s) with name {} is ready for operations!", kafkaConfigData.getTopicNamesToCreate().toArray());
        kafkaListenerEndpointRegistry.getListenerContainer("twitterTopicListener").start();
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
                messages.size(),
                keys.toString(),
                partitions.toString(),
                offsets.toString(),
                Thread.currentThread().getId());

        var twitterIndexModels = avroToElasticModelTransformer.getElasticModels(messages);
        var documentIds = elasticIndexClient.save(twitterIndexModels);

        log.info("Documents saved to elasticsearch with ids {}", documentIds.toArray());
    }
}
