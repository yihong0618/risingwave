package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;

import java.io.IOException;
import java.util.Iterator;

public class UpsertIcebergSink2 extends SinkBase {
    private final UpsertIcebergTaskWriterFactory factory;

    private UpsertIcebergTaskWriter taskWriter;

    public UpsertIcebergSink2(UpsertIcebergTaskWriterFactory factory, TableSchema rwSchema) {
        super(rwSchema);
        this.factory = factory;
        this.taskWriter = factory.create();
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        try {
            while (rows.hasNext()) {
                var row = rows.next();
                taskWriter.write(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
        flush();
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
                this.taskWriter.complete();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        this.taskWriter = null;
    }
}
