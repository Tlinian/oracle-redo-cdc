package com.example.redo.deserialize;

import com.example.redo.model.Xid;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class DdlEvent implements RedoEvent{
    private String sql;
    private long scn;
    private long commitScn;
    private EventType eventType;
    private Xid xid;

    @Override
    public EventType getEventType() {
        return EventType.DDL;
    }
}
