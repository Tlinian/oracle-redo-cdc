package com.example.redo.deserialize;

import com.example.redo.metadata.TableId;
import com.example.redo.model.Xid;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
public class DmlEvent extends RedoEvent{
    private long scn;
    private long blk;
    private long offset;
    private long seq;
    private Xid xid;
    int[] beforeCols;
    int[] afterCols;
    int objId;
    TableId tableId;
    List<Object> after = new ArrayList<>();
    List<Object> before = new ArrayList<>();

    public DmlEvent(long scn, long blk, long offset, long seq, Xid xid, long commitScn, EventType type, TableId tableId, int objId, List<Object> before, List<Object> after) {
        super(scn, commitScn, type);
        this.tableId = tableId;
        this.objId = objId;
        this.before = before;
        this.after = after;
        this.blk = blk;
        this.offset = offset;
        this.seq = seq;
        this.xid = xid;
    }
}
