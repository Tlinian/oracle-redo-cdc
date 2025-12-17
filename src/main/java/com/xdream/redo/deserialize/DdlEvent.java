package com.xdream.redo.deserialize;

import com.xdream.redo.model.Xid;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class DdlEvent implements RedoEvent{
    private String sql;
    private long scn;
    private long commitScn;
    private EventType eventType;
    private Xid xid;
    private int kind;

    @Override
    public EventType getEventType() {
        return EventType.DDL;
    }
}
