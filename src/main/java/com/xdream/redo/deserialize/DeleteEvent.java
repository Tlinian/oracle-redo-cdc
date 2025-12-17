package com.xdream.redo.deserialize;

import com.xdream.redo.metadata.TableId;
import com.xdream.redo.model.Xid;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@ToString
@Builder
public class DeleteEvent extends DmlEvent{
    protected TableId tableId;
    int[] beforeCols;
    List<Object> before;
    protected Xid xid;
    protected int objId;
    protected long commitScn;
    protected long scn;

     @Override
    public EventType getEventType() {
        return EventType.DELETE;
    }
}
