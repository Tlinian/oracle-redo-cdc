package com.example.redo.metadata;

public class BinaryColumnMeta extends ColumnMetaBase{
    public BinaryColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        return data;
    }
}
