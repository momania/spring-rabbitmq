package com.rabbitmq.spring.template;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ASyncRabbitTemplate extends RabbitTemplate implements DisposableBean {

    private final Log log = LogFactory.getLog(ASyncRabbitTemplate.class);
    private final BlockingQueue<RabbitMessage> queue = new LinkedBlockingQueue<RabbitMessage>();

    private volatile boolean running = true;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        new Thread(new Worker()).start();
    }

    @Override
    public void destroy() throws Exception {
        running = false;
        queue.clear();
    }

    @Override
    public void send(Serializable object, String routingKey, boolean mandatory, boolean direct) {
        queue.add(new RabbitMessage(object, routingKey, mandatory, direct));
    }

    private void sendMessage(RabbitMessage message) {
        super.send(message.getObject(), message.routingKey, message.isMandatory(), message.isDirect());
    }

    private final class Worker implements Runnable {

        @Override
        public void run() {
            while (running) {
                try {
                    RabbitMessage message = queue.poll(1, TimeUnit.SECONDS);
                    if (message != null) {
                        sendMessage(message);
                    }
                } catch (InterruptedException ie) {
                    if (log.isDebugEnabled()) {
                        log.debug("Interrupted while waiting for RabbitMessage in queue");
                    }
                } catch (Exception e) {
                    log.error("Error sending message", e);
                }
            }
        }
    }
    private final class RabbitMessage {
        private final Serializable object;
        private final String routingKey;
        private final boolean mandatory;
        private final boolean direct;

        private RabbitMessage(Serializable object, String routingKey, boolean mandatory, boolean direct) {
            this.object = object;
            this.routingKey = routingKey;
            this.mandatory = mandatory;
            this.direct = direct;
        }

        public Serializable getObject() {
            return object;
        }

        public String getRoutingKey() {
            return routingKey;
        }

        public boolean isMandatory() {
            return mandatory;
        }

        public boolean isDirect() {
            return direct;
        }
    }
}
