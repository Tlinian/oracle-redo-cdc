package com.example.redo.model;

import java.util.List;

public record RedoParseResult(
        int blockSize,
        List<RedoRecord> records,
        List<RedoChange> dml,
        List<RedoChange> ddl
) {
}

