package com.example.redo.metadata;

import oracle.sql.BINARY_DOUBLE;
import oracle.sql.DATE;

import java.sql.SQLException;

public class DateColumnMeta extends ColumnMetaBase{
    public DateColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        return new DATE(data).dateValue();
    }
}
