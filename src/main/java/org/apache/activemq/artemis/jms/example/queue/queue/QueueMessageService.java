package org.apache.activemq.artemis.jms.example.queue.queue;


import org.apache.activemq.artemis.jms.example.ReplyWaitingHandler;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class QueueMessageService {
    private static final int MAX_CAPACITY = 5;
    private List<QueueMessage> messsageQueue;
    private ReplyWaitingHandler replyWaitingHandler;
    private List<QueueMessageHandler> handlers;

    public void initialize(ConnectionFactory cf, Queue requestQueue) throws Exception {
        this.replyWaitingHandler = new ReplyWaitingHandler();
        messsageQueue = new ArrayList<>();
        handlers = new ArrayList<>();
        for ( int i = 0 ; i < MAX_CAPACITY; ++i) {
            QueueMessageHandler queueMessageHandler = new QueueMessageHandler(cf, requestQueue, messsageQueue, replyWaitingHandler);
            handlers.add(queueMessageHandler);
            queueMessageHandler.start();
        }
    }
    public void close() throws InterruptedException, JMSException {
        synchronized (messsageQueue) {
            while(messsageQueue.size() >0) {
                messsageQueue.wait();
            }
        }
        for ( QueueMessageHandler queueMessageHandler: handlers) {
            queueMessageHandler.stopHandler();
            queueMessageHandler.interrupt();
            queueMessageHandler.join();
            queueMessageHandler.close();
        }
    }

    public void addMessage(QueueMessageHandlerProducer producer, Optional<String> responseKeyOpt, Object data) throws InterruptedException {
        synchronized (messsageQueue) {
            while(messsageQueue.size() >= MAX_CAPACITY) {
                messsageQueue.wait();
            }
            responseKeyOpt.ifPresent(replyWaitingHandler::put);
            QueueMessage queueMessage = new QueueMessage(data, producer, responseKeyOpt);
            messsageQueue.add(queueMessage);
            messsageQueue.notifyAll();
        }
    }

    public Object getReply(String responseKey) throws InterruptedException {
        return replyWaitingHandler.getReply(responseKey);
    }
}
