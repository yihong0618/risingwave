package com.risingwave.connector;

import static com.google.common.base.Preconditions.checkNotNull;

import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.types.Types;

public class SinkRowProjection implements SinkRow {
    private final int[] mappedIndex;

    private SinkRow row;

    private SinkRowProjection(int[] mappedIndex) {
        checkNotNull(mappedIndex);
        this.mappedIndex = mappedIndex;
    }

    public static SinkRowProjection create(Types.StructType schema, Types.StructType deleteSchema) {
        var fieldIdToPosition =
                IntStream.range(0, schema.fields().size())
                        .boxed()
                        .collect(
                                Collectors.toMap(
                                        idx -> schema.fields().get(idx).fieldId(),
                                        Function.identity()));

        int[] mappedIndex =
                deleteSchema.fields().stream()
                        .mapToInt(field -> fieldIdToPosition.get(field.fieldId()))
                        .toArray();

        return new SinkRowProjection(mappedIndex);
    }

    public SinkRowProjection wrap(SinkRow row) {
        this.row = row;
        return this;
    }

    @Override
    public Object get(int index) {
        return row.get(this.mappedIndex[index]);
    }

    @Override
    public Data.Op getOp() {
        return row.getOp();
    }

    @Override
    public int size() {
        return this.mappedIndex.length;
    }

    @Override
    public void close() throws Exception {
        if (row != null) {
            row.close();
        }
    }
}
