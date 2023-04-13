package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

public class SinkRowWrapper implements StructLike {
    private final TableSchema rwSchema;
    private final Types.StructType icebergType;
    private SinkRow row = null;

    public SinkRowWrapper(TableSchema rwSchema, Types.StructType icebergType) {
        this.rwSchema = rwSchema;
        this.icebergType = icebergType;
    }

    public SinkRowWrapper wrap(SinkRow row) {
        this.row = row;
        return this;
    }

    @Override
    public int size() {
        return rwSchema.getNumColumns();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        var obj = row.get(pos);
        if (obj != null) {
            return javaClass.cast(obj);
        } else {
            return null;
        }
    }

    @Override
    public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException(
                "Could not set a field in the SinkRowWrapper because rowData is read-only");
    }
}
