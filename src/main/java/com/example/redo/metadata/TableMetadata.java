package com.example.redo.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@Getter
@ToString
public class TableMetadata {
    private TableId tableId;

    private Map<Integer,ColumnMeta> columnIdMap = new HashMap<>();
}
