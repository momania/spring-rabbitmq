package com.rabbitmq.spring;

public class UnroutableException extends RuntimeException {
    
    public UnroutableException() {
    }

    public UnroutableException(String message) {
        super(message);
    }

    public UnroutableException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnroutableException(Throwable cause) {
        super(cause);
    }
}
