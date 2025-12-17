package com.xdream.redo;

import com.xdream.redo.model.Xid;
import lombok.*;

@Getter
@ToString
@Setter
public class ConvertLlbRedoRecord extends ConvertRedoRecord {
    private int obj;
    private Xid xid;
    private int columnId;
    private int lSize;


}
