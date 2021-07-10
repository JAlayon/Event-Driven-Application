package com.alayon.microservices.demo.twitter.to.kafka.service.listener;

import com.alayon.microservices.demo.config.KafkaConfigData;
import com.alayon.microservices.demo.kafka.producer.config.service.KafkaProducer;
import com.alayon.microservices.demo.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private static final Logger log = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;


    public TwitterKafkaStatusListener(final KafkaConfigData kafkaConfigData,
                                      final KafkaProducer<Long, TwitterAvroModel> kafkaProducer,
                                      final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.twitterStatusToAvroTransformer = twitterStatusToAvroTransformer;
    }

    @Override
    public void onStatus(final Status status) {
        log.info("Received status text {}, sending to kafka topic {}", status.getText(), kafkaConfigData.getTopicName());
        var twitterAvroModel= twitterStatusToAvroTransformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }
}
