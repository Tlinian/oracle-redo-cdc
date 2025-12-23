package com.xdream.redo.metadata;

import oracle.sql.DATE;

public class DateColumnMeta extends ColumnMetaBase{
    public DateColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        if (data.length == 0) {
            return null;
        }
        return new DATE(data).dateValue().toString();
    }
}
