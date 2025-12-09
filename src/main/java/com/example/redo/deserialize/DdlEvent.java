package com.example.redo.deserialize;

public class DdlEvent extends RedoEvent{
    private String sql;
    public DdlEvent(long scn, long commitScn, EventType type, String sql) {
        super(scn, commitScn, type);
        this.sql = sql;
    }
    public String getSql() {
        return sql;
    }
}
