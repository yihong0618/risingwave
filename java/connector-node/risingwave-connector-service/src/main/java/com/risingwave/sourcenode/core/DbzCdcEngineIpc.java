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

import com.risingwave.sourcenode.types.CdcChunk;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.heartbeat.Heartbeat;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DbzCdcEngineIpc implements Runnable {
    static final int DEFAULT_QUEUE_CAPACITY = 16;

    private final DebeziumEngine<?> engine;
    private final DbzCdcEventConsumerIpc consumer;
    private final long id;

    /** If config is not valid will throw exceptions */
    public DbzCdcEngineIpc(long id, Properties config, DebeziumEngine.CompletionCallback callback) {
        var dbzHeartbeatPrefix = config.getProperty(Heartbeat.HEARTBEAT_TOPICS_PREFIX.name());
        var consumer =
                new DbzCdcEventConsumerIpc(
                        id, dbzHeartbeatPrefix, new ArrayBlockingQueue<>(DEFAULT_QUEUE_CAPACITY));

        // Builds a debezium engine but not start it
        this.id = id;
        this.consumer = consumer;
        this.engine =
                DebeziumEngine.create(Connect.class)
                        .using(config)
                        .using(callback)
                        .notifying(consumer)
                        .build();
    }

    /** Start to run the cdc engine */
    public void run() {
        engine.run();
    }

    public long getId() {
        return id;
    }

    public void stop() throws Exception {
        engine.close();
    }

    public BlockingQueue<CdcChunk> getOutputChannel() {
        return consumer.getOutputChannel();
    }
}
