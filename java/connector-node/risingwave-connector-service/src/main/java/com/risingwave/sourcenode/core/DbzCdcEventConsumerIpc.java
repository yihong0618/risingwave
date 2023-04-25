// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.sourcenode.core;

import com.risingwave.connector.cdc.debezium.internal.DebeziumOffset;
import com.risingwave.connector.cdc.debezium.internal.DebeziumOffsetSerializer;
import com.risingwave.sourcenode.types.CdcChunk;
import com.risingwave.sourcenode.types.CdcMessage;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbzCdcEventConsumerIpc
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    static final Logger LOG = LoggerFactory.getLogger(DbzCdcEventConsumerIpc.class);

    private final BlockingQueue<CdcChunk> outputChannel;
    private final long sourceId;
    private final JsonConverter converter;
    private final String heartbeatTopicPrefix;

    DbzCdcEventConsumerIpc(
            long sourceId, String heartbeatTopicPrefix, BlockingQueue<CdcChunk> store) {
        this.sourceId = sourceId;
        this.outputChannel = store;
        this.heartbeatTopicPrefix = heartbeatTopicPrefix;

        var jsonConverter = new JsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        // only serialize the value part
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        // include record schema
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        jsonConverter.configure(configs);
        this.converter = jsonConverter;
    }

    private boolean isHeartbeatEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null
                && heartbeatTopicPrefix != null
                && topic.startsWith(heartbeatTopicPrefix);
    }

    @Override
    public void handleBatch(
            List<ChangeEvent<SourceRecord, SourceRecord>> events,
            DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
            throws InterruptedException {
        CdcChunk chunk = new CdcChunk();
        for (ChangeEvent<SourceRecord, SourceRecord> event : events) {
            var record = event.value();
            if (isHeartbeatEvent(record)) {
                // skip heartbeat events
                continue;
            }
            // ignore null record
            if (record.value() == null) {
                committer.markProcessed(event);
                continue;
            }
            byte[] payload =
                    converter.fromConnectData(record.topic(), record.valueSchema(), record.value());

            // serialize the offset to a JSON, so that kernel doesn't need to
            // aware the layout of it
            DebeziumOffset offset =
                    new DebeziumOffset(record.sourcePartition(), record.sourceOffset());
            String offsetStr = "";
            try {
                byte[] serialized = DebeziumOffsetSerializer.INSTANCE.serialize(offset);
                offsetStr = new String(serialized, StandardCharsets.UTF_8);
            } catch (IOException e) {
                LOG.warn("failed to serialize debezium offset", e);
            }
            CdcMessage message =
                    new CdcMessage(new String(payload), String.valueOf(sourceId), offsetStr);
            LOG.debug("record => {}", message.getPayload());
            chunk.addMessage(message);
            committer.markProcessed(event);
        }
        chunk.setSourceId(sourceId);
        outputChannel.put(chunk);
        committer.markBatchFinished();
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return DebeziumEngine.ChangeConsumer.super.supportsTombstoneEvents();
    }

    public BlockingQueue<CdcChunk> getOutputChannel() {
        return this.outputChannel;
    }
}
