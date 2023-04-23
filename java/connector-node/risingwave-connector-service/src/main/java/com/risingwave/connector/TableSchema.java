package com.risingwave.connector;

import com.risingwave.proto.Data.DataType.TypeName;
import java.util.List;

public class TableSchema {
    public class Column {
        public String name;
        public TypeName dataType;

        public Column(String name, TypeName dataType) {
            this.name = name;
            this.dataType = dataType;
        }
    }

    public List<Column> columns;
    public List<Integer> pkIndices;
}
