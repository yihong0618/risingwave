package com.risingwave.sourcenode.types;

import com.risingwave.connector.api.source.SourceTypeE;

public enum SourceType {
    Unspecified,
    Mysql,
    Postgres,
    Citus;

    public static SourceTypeE toSourceTypeE(SourceType type) {
        switch (type) {
            case Mysql:
                return SourceTypeE.MYSQL;
            case Postgres:
                return SourceTypeE.POSTGRES;
            case Citus:
                return SourceTypeE.CITUS;
            default:
                return SourceTypeE.INVALID;
        }
    }
}
