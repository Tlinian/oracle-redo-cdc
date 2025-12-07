package com.example.redo.model;

import java.util.List;

public record RowData(
        int flag,
        int lockByte,
        int columnCount,
        List<ColumnData> columns
) {
}

