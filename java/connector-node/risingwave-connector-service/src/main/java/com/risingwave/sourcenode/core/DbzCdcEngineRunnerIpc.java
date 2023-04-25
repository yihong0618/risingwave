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

import com.risingwave.sourcenode.common.DbzConnectorConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single-thread engine runner */
public class DbzCdcEngineRunnerIpc {
    static final Logger LOG = LoggerFactory.getLogger(DbzCdcEngineRunnerIpc.class);

    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final DbzCdcEngineIpc engine;

    public DbzCdcEngineRunnerIpc(DbzCdcEngineIpc engine) {
        this.executor = Executors.newSingleThreadExecutor();
        this.engine = engine;
    }

    public static DbzCdcEngineRunnerIpc newCdcEngineRunnerIpc(DbzConnectorConfig config)
            throws Exception {
        DbzCdcEngineRunnerIpc runner = null;
        DbzCdcEngineIpc engine =
                new DbzCdcEngineIpc(
                        config.getSourceId(),
                        config.getResolvedDebeziumProps(),
                        (success, message, error) -> {
                            if (!success) {
                                LOG.error(
                                        "the engine terminated with error. message: {}",
                                        message,
                                        error);
                            }
                        });

        runner = new DbzCdcEngineRunnerIpc(engine);
        return runner;
    }

    /** Start to run the cdc engine */
    public void start() {
        if (isRunning()) {
            LOG.info("CdcEngine#{} already started", engine.getId());
            return;
        }

        executor.execute(engine);
        running.set(true);
        LOG.info("CdcEngine#{} started", engine.getId());
    }

    public void stop() throws Exception {
        if (isRunning()) {
            engine.stop();
            cleanUp();
            LOG.info("CdcEngine#{} terminated", engine.getId());
        }
    }

    public DbzCdcEngineIpc getEngine() {
        return engine;
    }

    public boolean isRunning() {
        return running.get();
    }

    private void cleanUp() {
        running.set(false);
        executor.shutdownNow();
    }
}
