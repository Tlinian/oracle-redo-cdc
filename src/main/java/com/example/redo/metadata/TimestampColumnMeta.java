package com.example.redo.metadata;

import oracle.sql.DATE;
import oracle.sql.TIMESTAMP;

import java.sql.SQLException;

public class TimestampColumnMeta extends ColumnMetaBase{
    public TimestampColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        try {
            return new TIMESTAMP(data).timestampValue();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
