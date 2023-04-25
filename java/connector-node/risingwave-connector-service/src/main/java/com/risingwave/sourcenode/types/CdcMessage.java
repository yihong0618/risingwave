package com.risingwave.sourcenode.types;

public class CdcMessage {
    public String payload;
    public String partition;
    public String offset;

    public CdcMessage() {}

    public CdcMessage(String payload, String partition, String offset) {
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
    }

    public String getOffset() {
        return offset;
    }

    public String getPartition() {
        return partition;
    }

    public String getPayload() {
        return payload;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
