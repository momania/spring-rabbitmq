package net.momania.spring.rabbitmq.template;

import net.momania.spring.rabbitmq.ExchangeType;
import net.momania.spring.rabbitmq.channel.RabbitChannelFactory;
import com.rabbitmq.client.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.io.Serializable;

/**
 * N.B. Be careful: This class it NOT thread safe.
 * For thread save rabbit template, use: ASyncRabbitTemplate
 */
public class RabbitTemplate implements InitializingBean, ShutdownListener, ReturnListener {

    public static final ExchangeType DEFAULT_EXCHANGE_TYPE = ExchangeType.DIRECT;
    public static final String DEFAULT_ROUTING_KEY = "#";

    private final Log log = LogFactory.getLog(RabbitTemplate.class);

    private RabbitChannelFactory channelFactory;
    private String exchange;
    private ExchangeType exchangeType = DEFAULT_EXCHANGE_TYPE;
    private String routingKey = DEFAULT_ROUTING_KEY;
    private boolean mandatory;
    private boolean immediate;


    private Channel channel;

    public void send(Serializable object) {
        send(object, routingKey, mandatory, immediate);
    }

    public void send(Serializable object, String routingKey) {
        send(object, routingKey, mandatory, immediate);
    }

    public void send(Serializable object, boolean mandatory, boolean immediate) {
        send(object, routingKey, mandatory, immediate);
    }

    public void send(Serializable object, String routingKey, boolean mandatory, boolean immediate) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Sending object [%s] with routingKey [%s] - mandatory [%s] - immediate [%s]"
                    , object, routingKey, mandatory, immediate));
        }
        try {
            channel.basicPublish(exchange, routingKey, mandatory, immediate, null, SerializationUtils.serialize(object));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        
        exchangeType.validateRoutingKey(routingKey);

        connectChannel();
    }

    private void connectChannel() {
        if (channel == null || !channel.isOpen()) {
            try {
                channel = channelFactory.createChannel();
                channel.getConnection().addShutdownListener(this);
                channel.setReturnListener(this);
                channel.exchangeDeclare(exchange, exchangeType.toString());
                if (log.isInfoEnabled()) {
                    log.info(String.format("Connected to exchange [%s(%s)] - routingKey [%s]"
                            , exchange, exchangeType, routingKey));
                }
            } catch (IOException e) {
                log.warn("Unable to connect channel", e);
            }
        }
    }

    @Required
    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    @Required
    public void setChannelFactory(RabbitChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        if (log.isInfoEnabled()) {
            log.info(String.format("Channel connection lost for reason [%s]", cause.getReason()));
            log.info(String.format("Reference [%s]", cause.getReference()));
        }

        if (cause.isInitiatedByApplication()) {
            if (log.isInfoEnabled()) {
                log.info("Sutdown initiated by application");
            }
        } else if (cause.isHardError()) {
            log.error("Shutdown is a hard error, trying to reconnect the channel...");
            connectChannel();
        }
    }

    @Override
    public void handleBasicReturn(int replyCode, String replyText, String exchange, String routingKey
            , AMQP.BasicProperties properties, byte[] body) throws IOException {

        log.warn(String.format("Got message back from server [%d] - [%s]", replyCode, replyText));
        handleReturn(replyCode, replyText, exchange, routingKey, properties, body);
    }


    /**
     * Callback hook for returned messages, overwrite where needed.
     * Will only be called when sending with 'immediate' or 'mandatory' set to true.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public void handleReturn(int replyCode, String replyText, String exchange, String routingKey
            , AMQP.BasicProperties properties, byte[] body) {

    }

    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }

    public void setImmediate(boolean immediate) {
        this.immediate = immediate;
    }

    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }
}
