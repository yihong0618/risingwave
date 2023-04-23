package com.risingwave.connector;

import com.risingwave.proto.Data;

public class Column {
    public String name;
    public Data.DataType.TypeName dataType;

    public Column() {}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Data.DataType.TypeName getDataType() {
        return dataType;
    }

    public void setDataType(Data.DataType.TypeName dataType) {
        this.dataType = dataType;
    }
}
