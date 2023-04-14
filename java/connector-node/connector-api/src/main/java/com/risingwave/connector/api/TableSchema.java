// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.api;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.DataType.TypeName;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableSchema {
    private final ImmutableList<String> columnNames;
    private final ImmutableMap<String, TypeName> columns;
    private final ImmutableMap<String, Integer> columnIndices;

    private final ImmutableList<String> primaryKeys;

    @Deprecated
    public TableSchema(
            List<String> columnNames, List<TypeName> typeNames, List<String> primaryKeys) {
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.primaryKeys = ImmutableList.copyOf(primaryKeys);

        var columnsBuilder = ImmutableMap.<String, TypeName>builder() ;
        var columnIndicesBuilder = ImmutableMap.<String, Integer>builder();
        for (int i = 0; i < columnNames.size(); i++) {
            columnsBuilder.put(columnNames.get(i), typeNames.get(i));
            columnIndicesBuilder.put(columnNames.get(i), i);
        }

        this.columns = columnsBuilder.build();
        this.columnIndices = columnIndicesBuilder.build();
    }

    private TableSchema(TableSchema.Builder builder) {
        this.columnNames = builder.columnNames.build();
        this.primaryKeys = ImmutableList.copyOf(builder.primaryKeys);

        var columnTypes = builder.columnTypes.build();

        var columnsBuilder = ImmutableMap.<String, TypeName>builder() ;
        var columnIndicesBuilder = ImmutableMap.<String, Integer>builder();
        for (int i = 0; i < columnNames.size(); i++) {
            columnsBuilder.put(columnNames.get(i), columnTypes.get(i));
            columnIndicesBuilder.put(columnNames.get(i), i);
        }

        this.columns = columnsBuilder.build();
        this.columnIndices = columnIndicesBuilder.build();
    }

    public int getNumColumns() {
        return columns.size();
    }

    public int getColumnIndex(String columnName) {
        return columnIndices.get(columnName);
    }

    public TypeName getColumnType(String columnName) {
        return columns.get(columnName);
    }

    public Map<String, TypeName> getColumnTypes() {
        return new HashMap<>(columns);
    }

    public String[] getColumnNames() {
        return columnNames.toArray(new String[0]);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TableSchema getMockTableSchema() {
        return new TableSchema(
                Lists.newArrayList("id", "name"),
                Lists.newArrayList(TypeName.INT32, TypeName.VARCHAR),
                Lists.newArrayList("id"));
    }

    public static ConnectorServiceProto.TableSchema getMockTableProto() {
        return ConnectorServiceProto.TableSchema.newBuilder()
                .addColumns(
                        ConnectorServiceProto.TableSchema.Column.newBuilder()
                                .setName("id")
                                .setDataType(TypeName.INT32)
                                .build())
                .addColumns(
                        ConnectorServiceProto.TableSchema.Column.newBuilder()
                                .setName("name")
                                .setDataType(TypeName.VARCHAR)
                                .build())
                .addAllPkIndices(List.of(1))
                .build();
    }

    public Object getFromRow(String columnName, SinkRow row) {
        return row.get(columnIndices.get(columnName));
    }

    public static TableSchema fromProto(ConnectorServiceProto.TableSchema tableSchema) {
        return new TableSchema(
                tableSchema.getColumnsList().stream()
                        .map(ConnectorServiceProto.TableSchema.Column::getName)
                        .collect(Collectors.toList()),
                tableSchema.getColumnsList().stream()
                        .map(ConnectorServiceProto.TableSchema.Column::getDataType)
                        .collect(Collectors.toList()),
                tableSchema.getPkIndicesList().stream()
                        .map(i -> tableSchema.getColumns(i).getName())
                        .collect(Collectors.toList()));
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @Override
    public String toString() {
        return "TableSchema{"
                + "columnNames="
                + columnNames
                + ", columns="
                + columns
                + ", columnIndices="
                + columnIndices
                + ", primaryKeys="
                + primaryKeys
                + '}';
    }

    public static class Builder {
        private final ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        private final ImmutableList.Builder<TypeName> columnTypes = ImmutableList.builder();

        private List<String> primaryKeys = Collections.emptyList();

        public Builder addColumn(String columnName, TypeName columnType) {
            this.columnNames.add(columnName);
            this.columnTypes.add(columnType);

            return this;
        }

        public Builder withPrimaryKey(List<String> primaryKeys) {
            Preconditions.checkNotNull(primaryKeys, "Primary keys can't be null!");
            this.primaryKeys = primaryKeys;
            return this;
        }

        public TableSchema build() {
            return new TableSchema(this);
        }
    }
}
