package com.alayon.microservices.demo.elastic.query.web.client.exception;

public class ElasticQueryWebClientException extends RuntimeException{

    public ElasticQueryWebClientException() {
        super();
    }

    public ElasticQueryWebClientException(final String message) {
        super(message);
    }

    public ElasticQueryWebClientException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
