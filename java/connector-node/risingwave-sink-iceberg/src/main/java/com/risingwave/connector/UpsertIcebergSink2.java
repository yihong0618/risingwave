package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.java.utils.ConnectorNodeMetrics;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpsertIcebergSink2 extends SinkBase {
    private static final Logger LOG = LoggerFactory.getLogger(UpsertIcebergSink2.class);

    private final UpsertIcebergTaskWriterFactory factory;
    private UpsertIcebergTaskWriter taskWriter;
    private final ExecutorService workerPool;

    public UpsertIcebergSink2(UpsertIcebergTaskWriterFactory factory, TableSchema rwSchema) {
        super(rwSchema);
        this.factory = factory;
        this.taskWriter = factory.create();
        this.workerPool = ThreadPools.newWorkerPool("iceberg-upsert-sink2", 10);
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        try {
            while (rows.hasNext()) {
                var row = rows.next();
                taskWriter.write(row);
                ConnectorNodeMetrics.ICEBERG_UPSERT_SINK2_ROWS.inc();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
        ConnectorNodeMetrics.ICEBER_UPSERT_SINK2_COMMIT_TIMER.time(this::flush);
        this.taskWriter = this.factory.create();
    }

    @Override
    public void drop() {
        try {
            if (this.taskWriter != null) {
                this.taskWriter.close();
                this.taskWriter = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void flush() {
        if (this.taskWriter != null) {
            try {
                var result = this.taskWriter.complete();
                var rowDelta =
                        this.factory.getTable().newRowDelta().scanManifestsWith(this.workerPool);
                Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
                Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

                rowDelta.commit(); // abort is automatically called if this fails.
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        this.taskWriter = null;
    }
}
