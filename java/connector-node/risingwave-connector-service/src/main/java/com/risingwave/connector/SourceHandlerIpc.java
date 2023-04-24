package com.risingwave.connector;

import com.risingwave.proto.Data;
import com.risingwave.sourcenode.common.DbzConnectorConfig;
import com.risingwave.sourcenode.common.PostgresValidatorIpc;
import com.risingwave.sourcenode.types.*;
import io.grpc.Status;
import io.grpc.StatusException;
import java.io.IOException;
import java.sql.DriverManager;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SourceHandlerIpc {

    static final Logger LOG = LoggerFactory.getLogger(SourceHandlerIpc.class);

    public static String handleValidate(
            SourceType sourceType, Map<String, String> properties, TableSchema tableSchema) {
        try {
            validateDbProperties(sourceType, properties, tableSchema);
        } catch (Exception e) {
            LOG.error("failed to validate source", e);
            return e.toString();
        }
        return "";
    }

    private static void ensurePropNotNull(Map<String, String> props, String name) {
        if (!props.containsKey(name)) {
            throw new RuntimeException(
                    String.format("'%s' not found, please check the WITH properties", name));
        }
    }

    private static String getJdbcPrefix(SourceType sourceType) {
        switch (sourceType) {
            case Mysql:
                return "jdbc:mysql";
            case Postgres:
            case Citus:
                return "jdbc:postgresql";
            default:
                throw new RuntimeException("unknown source type: " + sourceType);
        }
    }

    private static void validateDbProperties(
            SourceType sourceType, Map<String, String> props, TableSchema tableSchema)
            throws Exception {
        ensurePropNotNull(props, DbzConnectorConfig.HOST);
        ensurePropNotNull(props, DbzConnectorConfig.PORT);
        ensurePropNotNull(props, DbzConnectorConfig.DB_NAME);
        ensurePropNotNull(props, DbzConnectorConfig.TABLE_NAME);
        String jdbcUrl =
                getJdbcPrefix(sourceType)
                        + "://"
                        + props.get(DbzConnectorConfig.HOST)
                        + ":"
                        + props.get(DbzConnectorConfig.PORT)
                        + "/"
                        + props.get(DbzConnectorConfig.DB_NAME);
        LOG.debug("validate jdbc url: {}", jdbcUrl);

        var sqlStmts = new Properties();
        try (var input =
                SourceHandlerIpc.class
                        .getClassLoader()
                        .getResourceAsStream("validate_sql.properties")) {
            sqlStmts.load(input);
        } catch (IOException e) {
            LOG.error("failed to load sql statements", e);
            throw new RuntimeException(e);
        }

        ensurePropNotNull(props, DbzConnectorConfig.USER);
        ensurePropNotNull(props, DbzConnectorConfig.PASSWORD);
        String dbUser = props.get(DbzConnectorConfig.USER);
        String dbPassword = props.get(DbzConnectorConfig.PASSWORD);
        switch (sourceType) {
            case Postgres:
                ensurePropNotNull(props, DbzConnectorConfig.PG_SCHEMA_NAME);
                try (var validator =
                        new PostgresValidatorIpc(
                                jdbcUrl, dbUser, dbPassword, props, sqlStmts, tableSchema)) {
                    validator.validateAll();
                }
                break;

            case Citus:
                ensurePropNotNull(props, DbzConnectorConfig.PG_SCHEMA_NAME);
                try (PostgresValidatorIpc coordinatorValidator =
                        new PostgresValidatorIpc(
                                jdbcUrl, dbUser, dbPassword, props, sqlStmts, tableSchema)) {
                    coordinatorValidator.validateDistributedTable();
                    coordinatorValidator.validateTableSchema();
                }

                ensurePropNotNull(props, DbzConnectorConfig.DB_SERVERS);
                var servers = props.get(DbzConnectorConfig.DB_SERVERS);
                var workerAddrs = StringUtils.split(servers, ',');
                var jdbcPrefix = getJdbcPrefix(sourceType);
                for (String workerAddr : workerAddrs) {
                    String workerJdbcUrl =
                            jdbcPrefix
                                    + "://"
                                    + workerAddr
                                    + "/"
                                    + props.get(DbzConnectorConfig.DB_NAME);

                    LOG.info("workerJdbcUrl {}", workerJdbcUrl);
                    try (PostgresValidatorIpc workerValidator =
                            new PostgresValidatorIpc(
                                    workerJdbcUrl,
                                    dbUser,
                                    dbPassword,
                                    props,
                                    sqlStmts,
                                    tableSchema)) {
                        workerValidator.validateLogConfig();
                        workerValidator.validatePrivileges();
                    }
                }

                break;
            case Mysql:
                try (var conn =
                        DriverManager.getConnection(
                                jdbcUrl,
                                props.get(DbzConnectorConfig.USER),
                                props.get(DbzConnectorConfig.PASSWORD))) {
                    // usernamed and password are correct

                    LOG.debug("source schema: {}", tableSchema.columns);
                    LOG.debug("source pk: {}", tableSchema.pkIndices);

                    // check whether source db has enabled binlog
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_log"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("ON")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL doesn't enable binlog.\nPlease set the value of log_bin to 'ON' and restart your MySQL server."));
                            }
                        }
                    }
                    // check binlog format
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_format"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("ROW")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL binlog_format should be 'ROW'.\nPlease modify the config and restart your MySQL server."));
                            }
                        }
                    }
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_row_image"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("FULL")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL binlog_row_image should be 'FULL'.\nPlease modify the config and restart your MySQL server."));
                            }
                        }
                    }
                    // check whether table exist
                    try (var stmt = conn.prepareStatement(sqlStmts.getProperty("mysql.table"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.DB_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        while (res.next()) {
                            var ret = res.getInt(1);
                            if (ret == 0) {
                                throw new RuntimeException("MySQL table doesn't exist");
                            }
                        }
                    }
                    // check whether PK constraint match source table definition
                    try (var stmt =
                            conn.prepareStatement(sqlStmts.getProperty("mysql.table_schema"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.DB_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        var pkFields = new HashSet<String>();
                        int index = 0;
                        while (res.next()) {
                            var field = res.getString(1);
                            var dataType = res.getString(2);
                            var key = res.getString(3);

                            if (index >= tableSchema.columns.size()) {
                                throw new RuntimeException(("The number of columns mismatch"));
                            }

                            var srcCol = tableSchema.columns.get(index++);
                            if (!srcCol.name.equals(field)) {
                                throw new RuntimeException(
                                        String.format(
                                                "column name mismatch: %s, [%s]",
                                                field, srcCol.name));
                            }
                            if (!isMySQLDataTypeCompatible(dataType, srcCol.dataType)) {
                                throw new RuntimeException(
                                        String.format(
                                                "incompatible data type of column %s",
                                                srcCol.name));
                            }
                            if (key.equalsIgnoreCase("PRI")) {
                                pkFields.add(field);
                            }
                        }

                        if (!isPkMatch(tableSchema, pkFields)) {
                            throw new RuntimeException("Primary key mismatch");
                        }
                    }
                }
                break;
            default:
                LOG.warn("unknown source type");
                throw new RuntimeException("Unknown source type");
        }
    }

    private static boolean isPkMatch(TableSchema sourceSchema, Set<String> pkFields) {
        if (sourceSchema.pkIndices.size() != pkFields.size()) {
            return false;
        }
        for (var index : sourceSchema.pkIndices) {
            if (!pkFields.contains(sourceSchema.columns.get(index).name)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isMySQLDataTypeCompatible(
            String mysqlDataType, Data.DataType.TypeName typeName) {
        int val = typeName.getNumber();
        switch (mysqlDataType) {
            case "tinyint": // boolean
                return (val == Data.DataType.TypeName.BOOLEAN_VALUE)
                        || (Data.DataType.TypeName.INT16_VALUE <= val
                                && val <= Data.DataType.TypeName.INT64_VALUE);
            case "smallint":
                return Data.DataType.TypeName.INT16_VALUE <= val
                        && val <= Data.DataType.TypeName.INT64_VALUE;
            case "mediumint":
            case "int":
                return Data.DataType.TypeName.INT32_VALUE <= val
                        && val <= Data.DataType.TypeName.INT64_VALUE;
            case "bigint":
                return val == Data.DataType.TypeName.INT64_VALUE;

            case "float":
            case "real":
                return val == Data.DataType.TypeName.FLOAT_VALUE
                        || val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "double":
                return val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "decimal":
                return val == Data.DataType.TypeName.DECIMAL_VALUE;
            case "varchar":
                return val == Data.DataType.TypeName.VARCHAR_VALUE;
            default:
                return true; // true for other uncovered types
        }
    }
}
