package com.xdream.redo.metadata;

import oracle.sql.TIMESTAMP;

import java.sql.SQLException;

public class TimestampColumnMeta extends ColumnMetaBase{
    public TimestampColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        if (data.length == 0) {
            return null;
        }
        try {
            return new TIMESTAMP(data).timestampValue().toString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
