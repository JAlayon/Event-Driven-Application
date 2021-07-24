package com.alayon.microservices.demo.kafka.producer.config.service.impl;

import com.alayon.microservices.demo.kafka.producer.config.service.KafkaProducer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger log = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(final String topicName, final Long key, final TwitterAvroModel message) {
        log.info("Sending message='{}' to topic='{}'", message, topicName);
        var kafkaResultFuture = kafkaTemplate.send(topicName, key, message);
        addCallback(topicName, message, kafkaResultFuture);
    }

    @PreDestroy
    public void close(){
        if (kafkaTemplate != null){
            log.info("Closing kafka producer!");
            kafkaTemplate.destroy();
        }
    }

    private void addCallback(final String topicName,
                             final TwitterAvroModel message,
                             final ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
        kafkaResultFuture.addCallback(new ListenableFutureCallback<SendResult<Long, TwitterAvroModel>>() {
            @Override
            public void onFailure(final Throwable t) {
                log.error("Error while sending message {} to topic {}", message.toString(), topicName, t);
            }

            @Override
            public void onSuccess(final SendResult<Long, TwitterAvroModel> result) {
                RecordMetadata metadata = result.getRecordMetadata();
                log.debug("Received new metadata. Topic: {}; Partition: {}; Offset: {}; Timestamp: {}, At time: {}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        System.nanoTime()
                );
            }
        });
    }
}
