package org.apache.activemq.artemis.jms.example.queue.queue;


import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

public interface QueueMessageHandlerProducer {
    Object sendMessage(Session clientSession, MessageProducer producer, Object data);
    void close() throws JMSException;
}
