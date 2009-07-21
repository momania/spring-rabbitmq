package net.momania.spring.rabbitmq;

public class InvalidRoutingKeyException extends RuntimeException {

    public InvalidRoutingKeyException(String message) {
        super(message);
    }
}
