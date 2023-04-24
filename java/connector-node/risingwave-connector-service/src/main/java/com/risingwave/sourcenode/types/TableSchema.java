package com.risingwave.sourcenode.types;

import java.util.List;

public class TableSchema {

    public List<Column> columns;
    public List<Integer> pkIndices;

    public TableSchema() {}

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Integer> getPkIndices() {
        return pkIndices;
    }

    public void setPkIndices(List<Integer> pkIndices) {
        this.pkIndices = pkIndices;
    }
}
