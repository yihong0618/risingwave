package com.risingwave.sourcenode.types;

import java.util.ArrayList;
import java.util.List;

public class CdcChunk {
    long sourceId;
    List<CdcMessage> messages;

    public CdcChunk() {
        this.sourceId = 0;
        this.messages = new ArrayList<CdcMessage>();
    }

    public long getSourceId() {
        return sourceId;
    }

    public List<CdcMessage> getMessages() {
        return messages;
    }

    public void setSourceId(long sourceId) {
        this.sourceId = sourceId;
    }

    public void setMessages(List<CdcMessage> messages) {
        this.messages = messages;
    }

    public void addMessage(CdcMessage message) {
        this.messages.add(message);
    }
}
