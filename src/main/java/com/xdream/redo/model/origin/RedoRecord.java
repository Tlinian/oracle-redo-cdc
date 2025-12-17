package com.xdream.redo.model.origin;

import java.util.List;

public record RedoRecord(
        long blockNumber,
        long sequence,
        int offset,
        int length,
        int headerLength,
        int vld,
        long scn,
        int subScn,
        int conUid,
        List<RedoChange> changes,
        RedoChange change
) {
}

