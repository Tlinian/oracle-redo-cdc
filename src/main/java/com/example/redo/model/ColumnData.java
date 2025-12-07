package com.example.redo.model;

public record ColumnData(
        int length,
        boolean isNull,
        String hex,
        String ascii
) {
}

