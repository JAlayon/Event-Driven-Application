package com.alayon.microservices.demo.elastic.query.client.exception;

public class ElasticQueryClientException extends RuntimeException{

    public ElasticQueryClientException() {
    }

    public ElasticQueryClientException(final String message) {
        super(message);
    }

    public ElasticQueryClientException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
