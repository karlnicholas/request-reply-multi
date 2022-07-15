package org.apache.activemq.artemis.jms.example.queue.queue;

import java.util.Optional;

public class QueueMessage {
    private final Object message;
    private final QueueMessageHandlerProducer producer;
    private final Optional<String> responseKeyOpt;

    public QueueMessage(Object message, QueueMessageHandlerProducer producer, Optional<String> responseKeyOpt) {
        this.message = message;
        this.producer = producer;
        this.responseKeyOpt = responseKeyOpt;
    }

    public Object getMessage() {
        return message;
    }

    public QueueMessageHandlerProducer getProducer() {
        return producer;
    }

    public Optional<String> getResponseKeyOpt() {
        return responseKeyOpt;
    }
}
