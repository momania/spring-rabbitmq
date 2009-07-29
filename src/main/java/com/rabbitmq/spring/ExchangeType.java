package com.rabbitmq.spring;

public enum ExchangeType {

    DIRECT {
        @Override
        public void validateRoutingKey(String routingKey) {
            if (routingKey.contains("#") || routingKey.contains("*")) {
                throw new InvalidRoutingKeyException(
                        String.format("Routing key for exchange type %s may not contain wildcards (* of #)", this));
            }
        }
    },
    FANOUT,
    TOPIC;

    public void validateRoutingKey(String routingKey) {
        // do nothing
    }


    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
