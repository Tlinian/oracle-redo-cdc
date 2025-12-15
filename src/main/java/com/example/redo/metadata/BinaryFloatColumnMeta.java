package com.example.redo.metadata;

import oracle.sql.BINARY_FLOAT;

import java.sql.SQLException;

public class BinaryFloatColumnMeta extends ColumnMetaBase{
    public BinaryFloatColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        try {
            return new BINARY_FLOAT(data).floatValue();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
