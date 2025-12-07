package com.example.redo.model;

import java.util.List;

public record RedoRecord(
        long blockNumber,
        long sequence,
        int offset,
        int length,
        int headerLength,
        int vld,
        int scn,
        int subScn,
        List<RedoChange> changes
) {
}

