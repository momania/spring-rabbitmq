package com.rabbitmq.spring;

public class InvalidRoutingKeyException extends RuntimeException {

    public InvalidRoutingKeyException(String message) {
        super(message);
    }
}
