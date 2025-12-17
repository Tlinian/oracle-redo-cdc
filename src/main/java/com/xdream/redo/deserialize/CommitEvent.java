package com.xdream.redo.deserialize;

import com.xdream.redo.model.Xid;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class CommitEvent implements RedoEvent{
    private long scn;
    private long commitScn;
    private Xid xid;

     @Override
    public EventType getEventType() {
        return EventType.COMMIT;
    }
}
