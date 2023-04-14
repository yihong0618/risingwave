package com.risingwave.connector;

import com.risingwave.connector.api.sink.SinkRow;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.TripleWriter;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.parquet.Preconditions.checkState;

public class RwFileAppenderFactory implements FileAppenderFactory<SinkRow> {
    private final Schema schema;
    private final Map<String, String> props;
    private final PartitionSpec spec;
    private final int[] equalityFieldIds;
    private final Schema eqDeleteRowSchema;
    private final Schema posDeleteRowSchema;
    private final Table table;

    public RwFileAppenderFactory(Schema schema,
                                 Map<String, String> props,
                                 int[] equalityFieldIds,
                                 Schema eqDeleteRowSchema,
                                 Schema posDeleteRowSchema,
                                 Table table) {
        checkNotNull(table, "Table can't be null");
        this.schema = schema;
        this.props = props;
        this.spec = table.spec();
        this.equalityFieldIds = equalityFieldIds;
        this.eqDeleteRowSchema = eqDeleteRowSchema;
        this.posDeleteRowSchema = posDeleteRowSchema;
        this.table = table;
    }

    @Override
    public FileAppender<SinkRow> newAppender(OutputFile outputFile, FileFormat fileFormat) {
        try {
            if (Objects.requireNonNull(fileFormat) == FileFormat.PARQUET) {
                MetricsConfig metricsConfig = MetricsConfig.forTable(table);
                return Parquet.write(outputFile)
                        .createWriterFunc(msgType -> {
                            var parquetWriter = GenericParquetWriter.buildWriter(msgType);
                            return new RwParquetWriter(schema, parquetWriter);
                        })
                        .setAll(props)
                        .schema(schema)
                        .metricsConfig(metricsConfig)
                        .overwrite()
                        .build();
            }
            throw new UnsupportedOperationException(fileFormat + " not supported yet!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public DataWriter<SinkRow> newDataWriter(EncryptedOutputFile file, FileFormat format, StructLike partition) {
        return new DataWriter<>(
                newAppender(file.encryptingOutputFile(), format),
                format,
                file.encryptingOutputFile().location(),
                spec,
                partition,
                file.keyMetadata());
    }

    @Override
    public EqualityDeleteWriter<SinkRow> newEqDeleteWriter(EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        checkState(
                equalityFieldIds != null && equalityFieldIds.length > 0,
                "Equality field ids shouldn't be null or empty when creating equality-delete writer");
        checkNotNull(
                eqDeleteRowSchema,
                "Equality delete row schema shouldn't be null when creating equality-delete writer");

        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        try {
            if (Objects.requireNonNull(format) == FileFormat.PARQUET) {
                return Parquet.writeDeletes(outputFile.encryptingOutputFile())
                        .createWriterFunc(msgType -> {
                            var parquetWriter = GenericParquetWriter.buildWriter(msgType);
                            return new RwParquetWriter(eqDeleteRowSchema, parquetWriter);
                        })
                        .withPartition(partition)
                        .overwrite()
                        .setAll(props)
                        .metricsConfig(metricsConfig)
                        .rowSchema(eqDeleteRowSchema)
                        .withSpec(spec)
                        .withKeyMetadata(outputFile.keyMetadata())
                        .equalityFieldIds(equalityFieldIds)
                        .buildEqualityWriter();
            }
            throw new UnsupportedOperationException(
                    "Cannot write equality-deletes for unsupported file format: " + format);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PositionDeleteWriter<SinkRow> newPosDeleteWriter(EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);
        try {
            switch (format) {
                case PARQUET:
                    return Parquet.writeDeletes(outputFile.encryptingOutputFile())
                            .createWriterFunc(msgType -> {
                                var parquetWriter = GenericParquetWriter.buildWriter(msgType);
                                return new RwParquetWriter(posDeleteRowSchema, parquetWriter);
                            })
                            .withPartition(partition)
                            .overwrite()
                            .setAll(props)
                            .metricsConfig(metricsConfig)
                            .rowSchema(posDeleteRowSchema)
                            .withSpec(spec)
                            .withKeyMetadata(outputFile.keyMetadata())
                            .buildPositionWriter();

                default:
                    throw new UnsupportedOperationException(
                            "Cannot write pos-deletes for unsupported file format: " + format);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Record newRecord(Schema schema, SinkRow row) {
        Record record = GenericRecord.create(schema);
        for (int i = 0; i < schema.columns().size(); i++) {
            record.set(i, row.get(i));
        }
        return record;
    }

    private static class RwParquetWriter implements ParquetValueWriter<SinkRow> {
        private final Schema schema;
        private final ParquetValueWriter<Record> writer;

        private RwParquetWriter(Schema schema, ParquetValueWriter<Record> writer) {
            checkNotNull(schema, "Schema can't be null!");
            checkNotNull(writer, "Parquet writer can't be null!");
            this.writer = writer;
            this.schema = schema;
        }

        @Override
        public void write(int i, SinkRow sinkRow) {
            var record = newRecord(schema, sinkRow);
            writer.write(i, record);
        }

        @Override
        public List<TripleWriter<?>> columns() {
            return writer.columns();
        }

        @Override
        public void setColumnStore(ColumnWriteStore columnWriteStore) {
            writer.setColumnStore(columnWriteStore);
        }
    }
}
