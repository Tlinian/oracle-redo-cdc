package com.xdream.redo.metadata;

import oracle.sql.BINARY_DOUBLE;

import java.sql.SQLException;

public class BinaryDoubleColumnMeta extends ColumnMetaBase{
    public BinaryDoubleColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        try {
            return new BINARY_DOUBLE(data).doubleValue();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
