package com.alayon.microservices.demo.kafka.admin.exception;

/**
 * Exception class for kafka client error situations
 */
public class KafkaClientException extends RuntimeException{

    public KafkaClientException() {
    }

    public KafkaClientException(final String message) {
        super(message);
    }

    public KafkaClientException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
