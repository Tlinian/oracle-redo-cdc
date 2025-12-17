package com.xdream.redo.model;

import com.xdream.redo.model.origin.RedoChange;
import com.xdream.redo.model.origin.RedoRecord;

import java.util.List;

public record RedoParseResult(
        int blockSize,
        List<RedoRecord> records,
        List<RedoChange> dml,
        List<RedoChange> ddl
) {
}

