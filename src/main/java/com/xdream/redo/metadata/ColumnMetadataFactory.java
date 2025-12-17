package com.xdream.redo.metadata;

public class ColumnMetadataFactory {
    public static ColumnMeta createColumnMeta(String name, String type) {
        if (type.startsWith("NUMBER")||type.startsWith("FLOAT")) {
            return new NumberColumnMeta(name, type);
        }else if (type.startsWith("VARCHAR2")||type.startsWith("NVARCHAR2")||type.startsWith("CHAR")||type.startsWith("NCHAR")) {
            return new VarcharColumnMeta(name, type);
        }else if (type.startsWith("CLOB")||type.startsWith("LONG")||type.startsWith("NCLOB")) {
            return new VarcharColumnMeta(name, type);
        }else if (type.startsWith("BLOB")) {
            return new BinaryColumnMeta(name, type);
        }else if (type.startsWith("DATE")) {
            return new DateColumnMeta(name, type);
        }else if (type.startsWith("TIME")) {
            return new TimestampColumnMeta(name, type);
        }else if (type.startsWith("BINARY_FLOAT")) {
            return new BinaryFloatColumnMeta(name, type);
        }else if (type.startsWith("BINARY_DOUBLE")) {
            return new BinaryDoubleColumnMeta(name, type);
        }
        throw new UnsupportedOperationException("不支持的列类型: " + type);
    }
}
