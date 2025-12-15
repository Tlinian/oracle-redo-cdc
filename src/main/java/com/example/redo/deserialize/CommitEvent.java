package com.example.redo.deserialize;

import com.example.redo.model.Xid;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class CommitEvent implements RedoEvent{
    private String sql;
    private long scn;
    private long commitScn;
    private Xid xid;

     @Override
    public EventType getEventType() {
        return EventType.COMMIT;
    }
}
