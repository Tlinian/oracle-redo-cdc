package com.example.redo.model;

public enum OperationType {
    BEGIN_TX,
    COMMIT_TX,
    ROLLBACK_TX,
    UNDO,
    INSERT_ROW,
    UPDATE_ROW,
    DELETE_ROW,
    BLOCK_CLEANOUT,
    DDL,
    INDEX_OP,
    SEGMENT_OP,
    TABLESPACE_OP,
    UNKNOWN
}

