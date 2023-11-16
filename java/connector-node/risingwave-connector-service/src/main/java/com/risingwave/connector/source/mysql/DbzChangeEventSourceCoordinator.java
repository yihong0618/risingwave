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

package com.risingwave.connector.source.mysql;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.LoggingContext;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbzChangeEventSourceCoordinator<P extends Partition, O extends OffsetContext>
        extends ChangeEventSourceCoordinator<P, O> {
    static final Logger LOGGER = LoggerFactory.getLogger(DbzChangeEventSourceCoordinator.class);

    public DbzChangeEventSourceCoordinator(
            Offsets<P, O> previousOffsets,
            ErrorHandler errorHandler,
            Class<? extends SourceConnector> connectorType,
            CommonConnectorConfig connectorConfig,
            ChangeEventSourceFactory<P, O> changeEventSourceFactory,
            ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory,
            EventDispatcher<P, ?> eventDispatcher,
            DatabaseSchema<?> schema) {
        super(
                previousOffsets,
                errorHandler,
                connectorType,
                connectorConfig,
                changeEventSourceFactory,
                changeEventSourceMetricsFactory,
                eventDispatcher,
                schema);
    }

    @Override
    public synchronized void start(
            CdcSourceTaskContext taskContext,
            ChangeEventQueueMetrics changeEventQueueMetrics,
            EventMetadataProvider metadataProvider) {
        AtomicReference<LoggingContext.PreviousContext> previousLogContext =
                new AtomicReference<>();
        try {
            this.snapshotMetrics =
                    changeEventSourceMetricsFactory.getSnapshotMetrics(
                            taskContext, changeEventQueueMetrics, metadataProvider);
            this.streamingMetrics =
                    changeEventSourceMetricsFactory.getStreamingMetrics(
                            taskContext, changeEventQueueMetrics, metadataProvider);
            // running = true;
            Field field = ChangeEventSourceCoordinator.class.getDeclaredField("running");
            field.setAccessible(true);
            field.set(this, true);

            // run the snapshot source on a separate thread so start() won't block
            executor.submit(
                    () -> {
                        try {
                            previousLogContext.set(taskContext.configureLoggingContext("snapshot"));
                            LOGGER.info("Metrics registered (skipped)");

                            ChangeEventSource.ChangeEventSourceContext context =
                                    new ChangeEventSourceContextImpl();
                            LOGGER.info("Context created");

                            SnapshotChangeEventSource<P, O> snapshotSource =
                                    changeEventSourceFactory.getSnapshotChangeEventSource(
                                            snapshotMetrics);
                            executeChangeEventSources(
                                    taskContext,
                                    snapshotSource,
                                    previousOffsets,
                                    previousLogContext,
                                    context);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            LOGGER.warn("Change event source executor was interrupted", e);
                        } catch (Throwable e) {
                            errorHandler.setProducerThrowable(e);
                        } finally {
                            streamingConnected(false);
                        }
                    });
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        } finally {
            if (previousLogContext.get() != null) {
                previousLogContext.get().restore();
            }
        }
    }
}
