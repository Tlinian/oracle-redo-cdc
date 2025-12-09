package com.example.redo.metadata;

import oracle.sql.NUMBER;

import java.sql.SQLException;

public class NumberColumnMeta extends ColumnMetaBase{

    public NumberColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        try {
            return NUMBER.toBigDecimal(data);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
