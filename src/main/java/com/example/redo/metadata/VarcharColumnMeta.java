package com.example.redo.metadata;

public class VarcharColumnMeta extends ColumnMetaBase{
    public VarcharColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        return new String(data);
    }
}
