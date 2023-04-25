package com.risingwave.connector;

import com.risingwave.sourcenode.SourceValidateHandlerIpc;
import com.risingwave.sourcenode.core.DbzSourceHandlerIpc;
import com.risingwave.sourcenode.core.SourceHandlerFactoryIpc;
import com.risingwave.sourcenode.types.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// api for j4rs
public final class SourceHandlerIpc {

    static final Logger LOG = LoggerFactory.getLogger(SourceHandlerIpc.class);

    // returns empty string on success, returns
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

    // returns handler on success, returns null on failure
    public static DbzSourceHandlerIpc handleStart(
            Long sourceId,
            SourceType sourceType,
            String startOffset,
            Map<String, String> properties) {
        try {
            return SourceHandlerFactoryIpc.createSourceHandler(
                    SourceType.toSourceTypeE(sourceType), sourceId, startOffset, properties);
        } catch (Exception e) {
            LOG.error("failed to create the CDC engine", e);
        }
        return null;
    }
}
