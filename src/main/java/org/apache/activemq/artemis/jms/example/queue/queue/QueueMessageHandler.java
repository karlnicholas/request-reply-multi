package org.apache.activemq.artemis.jms.example.queue.queue;

import org.apache.activemq.artemis.jms.example.ReplyWaitingHandler;

import javax.jms.*;
import java.util.List;

class QueueMessageHandler extends Thread implements Runnable {
    private final Connection connection;
    private final List<QueueMessage> messsageQueue;
    private final Session clientSession;
    private final MessageProducer producer;
    private final ReplyWaitingHandler replyWaitingHandler;
    private boolean run;


    public QueueMessageHandler(ConnectionFactory cf, Queue requestQueue, List<QueueMessage> messsageQueue, ReplyWaitingHandler replyWaitingHandler) throws Exception {
        this.replyWaitingHandler = replyWaitingHandler;
        run = true;
        this.messsageQueue = messsageQueue;
        connection = cf.createConnection();
        // Step 6. Start the connection.
        connection.start();

        clientSession = connection.createSession();
        producer = clientSession.createProducer(requestQueue);
    }

    public void stopHandler() {
        run = false;
    }
    public void close() throws JMSException {
        connection.close();
    }

    @Override
    public void run() {
        while (run) {
            try {
                synchronized (messsageQueue) {
                    while (messsageQueue.isEmpty()) {
                        messsageQueue.wait();
                    }
                    QueueMessage queueMessage = messsageQueue.remove(0);
                    messsageQueue.notifyAll();
                    Object reply = queueMessage.getProducer().sendMessage(clientSession, producer, queueMessage.getMessage());
                    queueMessage.getResponseKeyOpt().ifPresent(key->replyWaitingHandler.handleReply(key, reply));
                }
            } catch (InterruptedException ex) {
                if ( run ) ex.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }
}
