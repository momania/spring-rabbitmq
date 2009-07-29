package com.rabbitmq.spring.remoting;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.MethodArgumentReader;
import com.rabbitmq.client.impl.MethodArgumentWriter;
import com.rabbitmq.utility.BlockingCell;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

class RabbitRpcClient {

    private final Channel channel;
    private final String exchange;
    private final String routingKey;

    private final Map<String, BlockingCell<Object>> continuationMap = new HashMap<String, BlockingCell<Object>>();
    private int correlationId;

    private final String replyQueue;
    private DefaultConsumer consumer;

    private final boolean mandatory;
    private final boolean immediate;
    private final int timeOutMs;

    public RabbitRpcClient(Channel channel, String exchange, String routingKey, int timeOutMs) throws IOException {
        this(channel, exchange, routingKey, timeOutMs, false, false);
    }

    @SuppressWarnings({"ConstructorWithTooManyParameters"})
    public RabbitRpcClient(Channel channel, String exchange, String routingKey, int timeOutMs, boolean mandatory
            , boolean immediate) throws IOException {
        this.channel = channel;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.timeOutMs = timeOutMs;
        this.mandatory = mandatory;
        this.immediate = immediate;
        correlationId = 0;

        replyQueue = setupReplyQueue();
        consumer = setupConsumer();
    }

    void checkConsumer() throws IOException {
        if (consumer == null) {
            throw new EOFException("RpcClient is closed");
        }
    }

    public void close() throws IOException {
        if (consumer != null) {
            channel.basicCancel(consumer.getConsumerTag());
            consumer = null;
        }
    }

    private String setupReplyQueue() throws IOException {
        return channel.queueDeclare("", false, false, true, true, null).getQueue();
    }

    private DefaultConsumer setupConsumer() throws IOException {
        DefaultConsumer consumer = new DefaultConsumer(channel) {

            @Override
            public void handleShutdownSignal(String consumerTag,
                                             ShutdownSignalException signal) {

                synchronized (continuationMap) {
                    for (Map.Entry<String, BlockingCell<Object>> entry : continuationMap.entrySet()) {
                        entry.getValue().set(signal);
                    }
                    RabbitRpcClient.this.consumer = null;
                }
            }

            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                synchronized (continuationMap) {
                    String replyId = properties.correlationId;
                    BlockingCell<Object> blocker = continuationMap.get(replyId);
                    continuationMap.remove(replyId);
                    blocker.set(body);
                }
            }
        };
        channel.basicConsume(replyQueue, true, consumer);
        return consumer;
    }

    void publish(AMQP.BasicProperties props, byte[] message)
            throws IOException {
        channel.basicPublish(exchange, routingKey, mandatory, immediate, props, message);

    }

    public byte[] primitiveCall(AMQP.BasicProperties props, byte[] message)
            throws IOException, ShutdownSignalException, TimeoutException {
        AMQP.BasicProperties localProps = props;
        checkConsumer();
        BlockingCell<Object> k = new BlockingCell<Object>();
        synchronized (continuationMap) {
            correlationId++;
            String replyId = "" + correlationId;
            if (localProps != null) {
                localProps.correlationId = replyId;
                localProps.replyTo = replyQueue;
            } else {
                localProps = new AMQP.BasicProperties(null, null, null, null,
                        null, replyId,
                        replyQueue, null, null, null,
                        null, null, null, null);
            }
            continuationMap.put(replyId, k);
        }
        publish(localProps, message);
        Object reply = k.uninterruptibleGet(timeOutMs);
        if (reply instanceof ShutdownSignalException) {
            ShutdownSignalException sig = (ShutdownSignalException) reply;
            ShutdownSignalException wrapper =
                    new ShutdownSignalException(sig.isHardError(),
                            sig.isInitiatedByApplication(),
                            sig.getReason(),
                            sig.getReference());
            wrapper.initCause(sig);
            throw wrapper;
        } else {
            return (byte[]) reply;
        }
    }

    public byte[] primitiveCall(byte[] message)
            throws IOException, ShutdownSignalException, TimeoutException {
        return primitiveCall(null, message);
    }

    public String stringCall(String message)
            throws IOException, ShutdownSignalException, TimeoutException {
        return new String(primitiveCall(message.getBytes()));
    }

    @SuppressWarnings({"IOResourceOpenedButNotSafelyClosed"})
    public Map<String, Object> mapCall(Map<String, Object> message)
            throws IOException, ShutdownSignalException, TimeoutException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        MethodArgumentWriter writer = new MethodArgumentWriter(new DataOutputStream(buffer));
        writer.writeTable(message);
        writer.flush();
        byte[] reply = primitiveCall(buffer.toByteArray());
        MethodArgumentReader reader =
                new MethodArgumentReader(new DataInputStream(new ByteArrayInputStream(reply)));
        return reader.readTable();
    }

    public Map<String, Object> mapCall(Object[] keyValuePairs)
            throws IOException, ShutdownSignalException, TimeoutException {
        Map<String, Object> message = new HashMap<String, Object>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            message.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return mapCall(message);
    }

    public Channel getChannel() {
        return channel;
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public Map<String, BlockingCell<Object>> getContinuationMap() {
        return Collections.unmodifiableMap(continuationMap);
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public String getReplyQueue() {
        return replyQueue;
    }

    public Consumer getConsumer() {
        return consumer;
    }
}
