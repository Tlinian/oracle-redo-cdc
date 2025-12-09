package com.example.redo.metadata;

public class ColumnMetadataFactory {
    public static ColumnMeta createColumnMeta(String name, String type) {
        if (type.startsWith("NUMBER")) {
            return new NumberColumnMeta(name, type);
        }else if (type.startsWith("VARCHAR2")) {
            return new VarcharColumnMeta(name, type);
        }
        throw new UnsupportedOperationException("不支持的列类型: " + type);
    }
}
