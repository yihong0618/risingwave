package com.risingwave.sourcenode.core;

import com.risingwave.sourcenode.types.CdcChunk;
import java.util.HashMap;

public class SourceHandlerManager {

    private static Long handlerId = 0L;

    public static HashMap<Long, DbzSourceHandlerIpc> handlers =
            new HashMap<Long, DbzSourceHandlerIpc>();

    public static Long insertHandler(DbzSourceHandlerIpc handler) {
        handlerId += 1;
        handlers.put(handlerId, handler);
        return handlerId;
    }

    public static CdcChunk getChunk(Long handlerId) {
        DbzSourceHandlerIpc handler = handlers.get(handlerId);
        if (handler == null) {
            throw new RuntimeException(String.format("handler not found: %d", handlerId));
        }
        return handler.getChunk();
    }
}
