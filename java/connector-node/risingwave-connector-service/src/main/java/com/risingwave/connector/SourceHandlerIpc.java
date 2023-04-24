package com.risingwave.connector;

import com.risingwave.sourcenode.SourceValidateHandlerIpc;
import com.risingwave.sourcenode.types.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SourceHandlerIpc {

    static final Logger LOG = LoggerFactory.getLogger(SourceHandlerIpc.class);

    public static String handleValidate(
            SourceType sourceType, Map<String, String> properties, TableSchema tableSchema) {
        try {
            SourceValidateHandlerIpc.validateDbProperties(sourceType, properties, tableSchema);
        } catch (Exception e) {
            LOG.error("failed to validate source", e);
            return e.toString();
        }
        return "";
    }
}
