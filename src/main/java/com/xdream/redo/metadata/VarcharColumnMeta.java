package com.xdream.redo.metadata;

public class VarcharColumnMeta extends ColumnMetaBase{
    public VarcharColumnMeta(String name, String type) {
        super(name, type);
    }

    @Override
    public Object convertData(byte[] data) {
        if (data.length == 0) {
            return null;
        }
        return new String(data);
    }
}
