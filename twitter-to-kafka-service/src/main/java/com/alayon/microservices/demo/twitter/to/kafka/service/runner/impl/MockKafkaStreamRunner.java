package com.alayon.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.alayon.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.alayon.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.alayon.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.alayon.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name="twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger log = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[]{
            "Lorem",
            "Ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "Maecenas",
            "portittor",
            "conque",
            "massa",
            "Fusse",
            "posuere",
            "magna",
            "sed",
            "pulvinar",
            "ultricies",
            "purus",
            "lectus",
            "libero"
    };

    private static final String TWEET_AS_RAW_JSON = "{" +
            "\"createdAt\":\"{0}\","+
            "\"id\":\"{1}\","+
            "\"text\":\"{2}\","+
            "\"user\":{\"id\":\"{3}\"}"+
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    public MockKafkaStreamRunner(final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
                                 final TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        var keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        var minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        var maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        var sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        log.info("Starting mock filtering streams for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(final String[] keywords,
                                       final Integer minTweetLength,
                                       final Integer maxTweetLength,
                                       final Long sleepTimeMs){
        Executors.newSingleThreadExecutor().submit(() -> {
            try{
                while (true){
                    var formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    var status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            }catch (TwitterException e){
                log.error("Error creating twitter status!", e);
            }
        });
    }

    private void sleep(final Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create!!", e);
        }
    }

    private String getFormattedTweet(final String[] keywords, final Integer minTweetLength, final Integer maxTweetLength) {
        var params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        var tweet = TWEET_AS_RAW_JSON;
        for (var i = 0; i < params.length; i++){
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(final String[] keywords, final Integer minTweetLength, final Integer maxTweetLength) {
        var tweet = new StringBuilder();
        var tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        for (var i = 0; i < tweetLength; i++){
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2)
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
        }
        return tweet.toString().trim();
    }
}
