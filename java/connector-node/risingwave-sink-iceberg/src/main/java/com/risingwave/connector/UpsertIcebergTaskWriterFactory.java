package com.risingwave.connector;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import org.apache.iceberg.*;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.*;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class UpsertIcebergTaskWriterFactory {
    private final Table table;
    private final Schema schema;
    private final TableSchema rwSchema;
    private final PartitionSpec spec;
    private final FileIO io;
    private final long targetFileSizeBytes;
    private final FileFormat format;
    private final List<Integer> equalityFieldIds;
    private final FileAppenderFactory<SinkRow> appenderFactory;

    private final OutputFileFactory outputFileFactory;


    public UpsertIcebergTaskWriterFactory(Table table,
                                          Schema schema,
                                          TableSchema rwSchema,
                                          long targetFileSizeBytes,
                                          FileFormat format) {
        this.table = table;
        this.schema = schema;
        this.rwSchema = rwSchema;
        this.spec = table.spec();
        this.io = table.io();
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.format = format;
        this.equalityFieldIds = rwSchema.getPrimaryKeys()
                .stream()
                .map(schema::findField)
                .map(Types.NestedField::fieldId)
                .collect(Collectors.toList());
        this.appenderFactory = new RwFileAppenderFactory(schema,
                Collections.emptyMap(),
                Ints.toArray(equalityFieldIds),
                TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds)),
                null,
                table);
        this.outputFileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();
    }

    public UpsertIcebergTaskWriter create() {
        return new UpsertIcebergTaskWriter(spec, format, appenderFactory, outputFileFactory, io, targetFileSizeBytes,
                schema, rwSchema, equalityFieldIds);
    }
}
