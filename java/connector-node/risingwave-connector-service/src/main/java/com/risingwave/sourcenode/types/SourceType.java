package com.risingwave.sourcenode.types;

import com.risingwave.connector.api.source.SourceTypeE;

public enum SourceType {
    UNSPECIFIED,
    MYSQL,
    POSTGRES,
    CITUS;

    public static SourceTypeE toSourceTypeE(SourceType type) {
        switch (type) {
            case MYSQL:
                return SourceTypeE.MYSQL;
            case POSTGRES:
                return SourceTypeE.POSTGRES;
            case CITUS:
                return SourceTypeE.CITUS;
            default:
                return SourceTypeE.INVALID;
        }
    }
}
