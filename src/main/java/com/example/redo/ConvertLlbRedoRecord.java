package com.example.redo;

import com.example.redo.model.ChangeCode;
import com.example.redo.model.Xid;
import lombok.*;

import java.util.List;

@Getter
@ToString
@Setter
public class ConvertLlbRedoRecord extends ConvertRedoRecord {
    private int obj;
    private Xid xid;
    private int columnId;
    private int lSize;


}
