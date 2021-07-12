package com.alayon.microservices.demo.kafka.admin.client;

import com.alayon.microservices.demo.config.KafkaConfigData;
import com.alayon.microservices.demo.config.RetryConfigData;
import com.alayon.microservices.demo.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public KafkaAdminClient(final KafkaConfigData kafkaConfigData,
                            final RetryConfigData retryConfigData,
                            final AdminClient adminClient,
                            final RetryTemplate retryTemplate,
                            final WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopics(){
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        }catch(Throwable t){
            throw new KafkaClientException("Reached max number of retry for creating kafka topic(s)!!", t);
        }
        checkTopicsCreated();
    }

    public void checkTopicsCreated(){
        var topics = getTopics();
        var retryCount = 1;
        var maxRetry = retryConfigData.getMaxAttempts();
        var multiplier = retryConfigData.getMultiplier().intValue();
        var sleepTimeMs = retryConfigData.getSleepTimeMs();
        for (String topic: kafkaConfigData.getTopicNamesToCreate()){
            while(!isTopicCreated(topics, topic)){
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    public void checkSchemaRegistry(){
        var retryCount = 1;
        var maxRetry = retryConfigData.getMaxAttempts();
        var multiplier = retryConfigData.getMultiplier().intValue();
        var sleepTimeMs = retryConfigData.getSleepTimeMs();
        while(!getSchemaRegistryStatus().is2xxSuccessful()){
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatus getSchemaRegistryStatus(){
        try {
            return webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
//                    .exchangeToMono(response -> response.bodyToMono(ClientResponse.class)
//                            .map(ClientResponse::statusCode)).block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    private CreateTopicsResult doCreateTopics(final RetryContext retryContext) {
        var topicNames = kafkaConfigData.getTopicNamesToCreate();
        log.info("Creating {} topic(s), attempt {}",topicNames.size(), retryContext.getRetryCount());
        var kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());

        return adminClient.createTopics(kafkaTopics);
    }

    private Collection<TopicListing> getTopics(){
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        }catch(Throwable t){
            throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!!", t);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(final RetryContext retryContext)
            throws ExecutionException, InterruptedException {
        log.info("Reading kafka topic {}, attempt {}",
                kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        var topics = adminClient.listTopics().listings().get();
        if (topics != null){
            topics.forEach(topic -> log.debug("Topic with name {}", topic.name()));
        }
        return topics;
    }

    private void sleep(final Long sleepTimeMs) {
        try{
            Thread.sleep(sleepTimeMs);
        }catch (InterruptedException e){
            throw new KafkaClientException("Error while sleeping for waiting new created topics!!");
        }
    }

    private void checkMaxRetry(final int currentRetry, final Integer maxRetry) {
        if (currentRetry > maxRetry)
            throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!");
    }

    private boolean isTopicCreated(final Collection<TopicListing> topics, final String topicName) {
        if (topics == null || topics.isEmpty())
            return false;
        return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
    }
}
