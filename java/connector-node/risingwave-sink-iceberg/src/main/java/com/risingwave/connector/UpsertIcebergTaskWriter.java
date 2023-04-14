package com.risingwave.connector;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.*;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Tasks;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

public class UpsertIcebergTaskWriter extends BaseTaskWriter<SinkRow> {
    private final Schema schema;
    private final Schema deleteSchema;

    private final SinkRowWrapper wrapper;

    private final SinkRowWrapper keyWrapper;

    private final SinkRowProjection keyProjection;

    private final PartitionKey partitionKey;

    private final Map<PartitionKey, SinkRowUpsertWriter> writers = Maps.newHashMap();

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
        this.wrapper = new SinkRowWrapper(rwSchema, schema.asStruct());
        this.keyWrapper = new SinkRowWrapper(convert(deleteSchema), deleteSchema.asStruct());
        this.keyProjection = SinkRowProjection.create(schema.asStruct(), deleteSchema.asStruct());

        this.partitionKey = new PartitionKey(spec, schema);
    }


    @Override
    public void write(SinkRow row) throws IOException {
        var writer = route(row);
        switch (row.getOp()) {
            case INSERT:
            case UPDATE_INSERT:
                writer.write(row);
                break;
            case DELETE:
            case UPDATE_DELETE:
                writer.deleteKey(keyProjection.wrap(row));
                break;
            default:
                throw new UnsupportedEncodingException("Unknow row op: " + row.getOp());
        }
    }

    private SinkRowUpsertWriter route(SinkRow row) {
        partitionKey.partition(wrapper.wrap(row));

        SinkRowUpsertWriter writer = writers.get(partitionKey);
        if (writer == null) {
            PartitionKey copiedKey = partitionKey.copy();
            writer = new SinkRowUpsertWriter(copiedKey);
            writers.put(copiedKey, writer);
        }

        return writer;
    }

    @Override
    public void close() throws IOException {
        try {
            Tasks.foreach(writers.values())
                    .throwFailureWhenFinished()
                    .noRetry()
                    .run(SinkRowUpsertWriter::close, IOException.class);

            writers.clear();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close equality delta writer", e);
        }
    }

    private static TableSchema convert(Schema icebergSchema) {
        checkNotNull(icebergSchema, "Iceberg schema can't be null!");
        var rwSchemaBuilder = TableSchema.builder();
        for (var field: icebergSchema.columns()) {
            var rwType = convert(field.type());
            rwSchemaBuilder.addColumn(field.name(), rwType);
        }

        return rwSchemaBuilder.withPrimaryKey(Lists.newArrayList(icebergSchema.identifierFieldNames().iterator()))
                .build();
    }

    private static Data.DataType.TypeName convert(Type icebergType) {
        switch (icebergType.typeId()) {
            case BOOLEAN: return Data.DataType.TypeName.BOOLEAN;
            case INTEGER: return Data.DataType.TypeName.INT32;
            case LONG: return Data.DataType.TypeName.INT64;
            case FLOAT: return Data.DataType.TypeName.FLOAT;
            case DOUBLE: return Data.DataType.TypeName.DOUBLE;
            case STRING: return Data.DataType.TypeName.VARCHAR;
            case BINARY: return Data.DataType.TypeName.BYTEA;
            case TIMESTAMP: return Data.DataType.TypeName.TIMESTAMP;
            default:
                throw new IllegalArgumentException("Can't convert iceberg type " + icebergType.typeId().name() + " to risingwave type!");
        }
    }

    private class SinkRowUpsertWriter extends BaseEqualityDeltaWriter {

        protected SinkRowUpsertWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(SinkRow data) {
            return wrapper.wrap(data);
        }

        @Override
        protected StructLike asStructLikeKey(SinkRow key) {
            return keyWrapper.wrap(key);
        }
    }
}
