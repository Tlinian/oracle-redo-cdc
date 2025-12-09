package com.example.redo.metadata;

public interface ColumnMeta {

    String getName();
    String getType();

    Object convertData(byte[] data);
}
