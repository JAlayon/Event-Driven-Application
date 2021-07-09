package com.alayon.microservices.demo.twitter.to.kafka.service;

import com.alayon.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.alayon.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final StreamRunner streamRunner;

    public TwitterToKafkaServiceApplication(final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
                                            final StreamRunner streamRunner) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(final String... args) throws Exception {
        log.info("App starts...");
        log.info(Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0])));
        log.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
        streamRunner.start();
    }
}
