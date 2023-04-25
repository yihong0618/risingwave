package com.risingwave.sourcenode.types;

import java.util.ArrayList;
import java.util.List;

public class CdcChunk {
    long sourceId;
    List<CdcMessage> events;

    public CdcChunk() {
        this.sourceId = 0;
        this.events = new ArrayList<CdcMessage>();
    }

    public long getSourceId() {
        return sourceId;
    }

    public List<CdcMessage> getEvents() {
        return events;
    }

    public void setSourceId(long sourceId) {
        this.sourceId = sourceId;
    }

    public void setEvents(List<CdcMessage> events) {
        this.events = events;
    }

    public void addEvent(CdcMessage event) {
        this.events.add(event);
    }
}
