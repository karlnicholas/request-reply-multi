package org.apache.activemq.artemis.jms.example;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReplyWaitingHandler {
    public static final int RESPONSE_TIMEOUT = 3000;
    public static final long TIMEOUT_MAX = 9_000_000_000L;
    private final ConcurrentMap<String, ReplyWaiting> repliesWaiting;

    public ReplyWaitingHandler() {
        repliesWaiting = new ConcurrentHashMap<>();
    }

    public void put(String responseKey) {
        repliesWaiting.put(responseKey, new ReplyWaiting( null, System.nanoTime()));
    }

    public Object getReply(String responseKey) throws InterruptedException {
        synchronized (repliesWaiting) {
            while (repliesWaiting.containsKey(responseKey) && repliesWaiting.get(responseKey).checkReply().isEmpty()) {
                repliesWaiting.wait(RESPONSE_TIMEOUT);
                if (System.nanoTime() - repliesWaiting.get(responseKey).getNanoTime() > TIMEOUT_MAX) {
                    System.out.println("getReply timeout");
                    break;
                }
            }
        }
        return repliesWaiting.remove(responseKey).getReply();
    }

    public void handleReply(String key, Object reply) {
        synchronized (repliesWaiting) {
            ReplyWaiting rw = repliesWaiting.get(key);
            rw.setReply(reply);
            repliesWaiting.notifyAll();
        }
    }
}
