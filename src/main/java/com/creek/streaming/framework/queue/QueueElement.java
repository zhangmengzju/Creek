package com.creek.streaming.framework.queue;

public class QueueElement {
    private long memberSeq = 0L;
    private long  itemId   = 0L;
    private long timestamp = 0L;

    public QueueElement(long memberSeq, long  itemId, long timestamp) {
       this.memberSeq = memberSeq;
       this.itemId = itemId;
       this.timestamp = timestamp;
    }

    public long getMemberSeq() {
        return memberSeq;
    }

    public void setMemberSeq(long memberSeq) {
        this.memberSeq = memberSeq;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
