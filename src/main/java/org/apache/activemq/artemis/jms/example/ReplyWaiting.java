package org.apache.activemq.artemis.jms.example;

import java.util.Optional;

public class ReplyWaiting {
    private Object reply;
    private long nanoTime;

    public ReplyWaiting(Object reply, long nanoTime) {
        this.reply = reply;
        this.nanoTime = nanoTime;
    }

    public Optional<Object> checkReply() {
        return Optional.ofNullable(reply);
    }

    public Object getReply() {
        return reply;
    }

    public void setReply(Object reply) {
        this.reply = reply;
    }

    public long getNanoTime() {
        return nanoTime;
    }
}
