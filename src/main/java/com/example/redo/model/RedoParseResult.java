package com.example.redo.model;

import com.example.redo.model.origin.RedoChange;
import com.example.redo.model.origin.RedoRecord;

import java.util.List;

public record RedoParseResult(
        int blockSize,
        List<RedoRecord> records,
        List<RedoChange> dml,
        List<RedoChange> ddl
) {
}

