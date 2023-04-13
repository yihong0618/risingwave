package com.risingwave.connector;

import com.google.common.collect.Sets;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import org.apache.iceberg.*;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;

import java.io.IOException;
import java.util.List;

public class UpsertIcebergTaskWriter extends BaseTaskWriter<SinkRow> {
    private final Schema schema;
    private final Schema deleteSchema;

    public UpsertIcebergTaskWriter(PartitionSpec spec,
                                   FileFormat format,
                                   FileAppenderFactory<SinkRow> appenderFactory,
                                   OutputFileFactory fileFactory,
                                   FileIO io,
                                   long targetFileSize,
                                   Schema schema,
                                   TableSchema rwSchema,
                                   List<Integer> equalityFieldIds) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.schema = schema;
        this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    }


    @Override
    public void write(SinkRow row) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    private class SinkRowUpsertWriter extends BaseEqualityDeltaWriter {

        protected SinkRowUpsertWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(SinkRow data) {
            return null;
        }

        @Override
        protected StructLike asStructLikeKey(SinkRow key) {
            return null;
        }
    }
}
