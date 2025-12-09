package com.example.redo.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@Getter
public class TableMetadata {
    private TableId tableId;

    private Map<Integer,ColumnMeta> columnIdMap = new HashMap<>();
}
