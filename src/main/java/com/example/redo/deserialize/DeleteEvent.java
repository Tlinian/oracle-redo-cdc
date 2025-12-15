package com.example.redo.deserialize;

import com.example.redo.metadata.TableId;
import com.example.redo.model.Xid;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@ToString
@Builder
public class DeleteEvent extends DmlEvent{
    int[] beforeCols;
    List<Object> before;
    protected Xid xid;
    protected int objId;
    protected TableId tableId;
    protected long commitScn;
    protected long scn;

     @Override
    public EventType getEventType() {
        return EventType.DELETE;
    }
}
