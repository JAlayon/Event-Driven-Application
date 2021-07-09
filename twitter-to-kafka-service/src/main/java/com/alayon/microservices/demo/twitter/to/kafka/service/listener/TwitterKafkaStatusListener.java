package com.alayon.microservices.demo.twitter.to.kafka.service.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private static final Logger log = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    @Override
    public void onStatus(final Status status) {
        log.info("Twitter status with text {}", status.getText());
    }
}
