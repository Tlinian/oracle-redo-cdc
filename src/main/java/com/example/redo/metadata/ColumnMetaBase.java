package com.example.redo.metadata;

public abstract class ColumnMetaBase implements ColumnMeta{
    private String name;
    private String type;
    public ColumnMetaBase(String name, String type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }
}
