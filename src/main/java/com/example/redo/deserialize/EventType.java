package com.example.redo.deserialize;

import com.example.redo.model.ChangeCode;

public enum EventType {
    INSERT,
    INSERT_MULTI,
    UPDATE,
    DELETE,
    DDL,
    COMMIT,
    BEGIN,
    UNKNOWN;

    public static EventType fromString(ChangeCode type) {
        return switch (type) {
            case INSERT -> INSERT;
            case DELETE -> DELETE;
            case UPDATE -> UPDATE;
            case COMMIT -> COMMIT;
            case DDL -> DDL;
            default -> UNKNOWN;
        };
    }
}
