package com.xdream.redo.metadata;

import oracle.sql.NUMBER;

import java.sql.SQLException;

public class NumberColumnMeta extends ColumnMetaBase{

    public NumberColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        if (data.length == 0) {
            return null;
        }
        try {
            return NUMBER.toBigDecimal(data);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
