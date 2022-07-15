/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import org.apache.activemq.artemis.jms.example.queue.queue.QueueMessageHandlerProducer;
import org.apache.activemq.artemis.jms.example.queue.queue.QueueMessageService;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.util.*;

/**
 * A simple JMS example that shows how to use Request/Replay style messaging.
 * <p>
 * Of course, in a real world example you would re-use the session, producer, consumer and temporary queue
 * and not create a new one for each message!
 * <p>
 * Or better still use the correlation id, and just store the requests in a map, then you don't need a temporary queue at all
 */
public class RequestReplyExample {

    public static void main(final String[] args) throws Exception {
        Connection connection = null;
        InitialContext initialContext = null;

        try {
            // Step 1. Start the request server
            SimpleRequestServer server = new SimpleRequestServer("queue/exampleQueue", "queue/exampleQueue2");
            server.start();

            // Step 1. Start the request server
            SimpleRequestServer server2 = new SimpleRequestServer("queue/exampleQueue2", null);
            server2.start();

            // Step 2. Create an initial context to perform the JNDI lookup.
            initialContext = new InitialContext();

            // Step 3. Lookup the queue for sending the request message
            Queue requestQueue = (Queue) initialContext.lookup("queue/exampleQueue");

            // Step 4. Lookup for the Connection Factory
            ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

            // Step 5. Create a JMS Connection
            connection = cf.createConnection();

            // Step 6. Start the connection.
            connection.start();

            // Step 7. Create a JMS Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            QueueMessageService queueMessageService = new QueueMessageService();
            queueMessageService.initialize(cf, requestQueue);
            // Step 9. Create a temporary queue used to send reply message
            List<Thread> messages = new ArrayList<>();
            MessageRequestResponse producer = new MessageRequestResponse(session);
            int count = 10;
            for ( int i=0; i < count; ++i) {
                messages.add(new NewMessageReceived(queueMessageService, producer));
            }
            for ( int i=0; i < count; ++i) {
                messages.get(i).start();
            }
            for ( int i=0; i < count; ++i) {
                messages.get(i).join();
            }
            // Step 19. Shutdown the request server
            queueMessageService.close();
            producer.close();
        } finally {
            // Step 20. Be sure to close our JMS resources!
            if (connection != null) {
                connection.close();
            }
            // Step 21. Also close the initialContext!
            if (initialContext != null) {
                initialContext.close();
            }
        }
    }
}
class NewMessageReceived extends Thread implements Runnable {
    private final QueueMessageService queueMessageService;
    private final MessageRequestResponse producer;

    public NewMessageReceived(QueueMessageService queueMessageService, MessageRequestResponse producer) {
        this.queueMessageService = queueMessageService;
        this.producer = producer;
    }

    @Override
    public void run() {
        String responseKey = UUID.randomUUID().toString();
        try {
            queueMessageService.addMessage(producer, Optional.of(responseKey), "Test Message " + UUID.randomUUID().toString());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}

class MessageRequestResponse implements QueueMessageHandlerProducer {

    private final Session session;
    private final MessageConsumer replyConsumer;
    private final Queue replyQueue;

    public MessageRequestResponse(Session session) throws JMSException {
        this.session = session;
        // Step 8. Create a JMS Message Producer to send request message
        // Step 10. Create consumer to receive reply message
        replyQueue = session.createTemporaryQueue();
        replyConsumer = session.createConsumer(replyQueue);
    }


    @Override
    public Object sendMessage(Session clientSession, MessageProducer producer, Object data) {
        try {

            // Step 11. Create a request Text Message
            TextMessage requestMsg = null;
            requestMsg = session.createTextMessage(String.valueOf(data));

            // Step 12. Set the ReplyTo header so that the request receiver knows where to send the reply.
            requestMsg.setJMSReplyTo(replyQueue);

            // Step 13. Sent the request message
            producer.send(requestMsg);

            // Step 15. Receive the reply message.
            TextMessage replyMessageReceived = (TextMessage) replyConsumer.receive();
            String m = replyMessageReceived.getText();
            System.out.println(m);
            return m;

        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws JMSException {
        replyConsumer.close();
    }
}

class SimpleRequestServer implements MessageListener {

    private Connection connection;

    private Session session;

    MessageProducer replyProducer;

    MessageConsumer requestConsumer;
    private final String queueName;
    private final String queueName2;
    private Queue forwardQueue;
    private MessageProducer forwardProducer;
    private Queue forwardReply;
    private MessageConsumer forwardConsumer;

    SimpleRequestServer(String queueName, String queueName2) {
        this.queueName = queueName;
        this.queueName2 = queueName2;
    }

    public void start() throws Exception {
        // Get an initial context to perform the JNDI lookup.
        InitialContext initialContext = new InitialContext();

        // Lookup the queue to receive the request message
        Queue requestQueue = (Queue) initialContext.lookup(queueName);

        // Lookup for the Connection Factory
        ConnectionFactory cfact = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

        // Create a connection
        connection = cfact.createConnection();

        // Start the connection;
        connection.start();

        // Create a session
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Lookup the queue to receive the request message
        if ( queueName2 != null ) {
            forwardQueue = (Queue) initialContext.lookup(queueName2);
            forwardProducer = session.createProducer(forwardQueue);
            forwardReply = session.createTemporaryQueue();
            forwardConsumer = session.createConsumer(forwardReply);
        }
        // Create a producer to send the reply message
        replyProducer = session.createProducer(null);

        // Create the request comsumer
        requestConsumer = session.createConsumer(requestQueue);

        // register the listener
        requestConsumer.setMessageListener(this);
    }

    @Override
    public void onMessage(final Message request) {
        try {
            // Extract the ReplyTo destination
            // Create the reply message
            TextMessage replyMessage;
            if ( forwardQueue != null ) {
                TextMessage forwardMessage = session.createTextMessage(" forwardReply message ");
                forwardMessage.setJMSReplyTo(forwardReply);
                forwardProducer.send(forwardMessage);
                replyMessage = (TextMessage) forwardConsumer.receive();
            } else {
                 replyMessage = session.createTextMessage(request.getBody(String.class) + " reply message");
            }
            Destination replyDestination = request.getJMSReplyTo();
            // Send out the reply message
            replyProducer.send(replyDestination, replyMessage);

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() throws JMSException {
        connection.close();
    }
}
