package com.example.redo;

import com.example.redo.model.ChangeCode;
import com.example.redo.model.Xid;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
@AllArgsConstructor
public class ConvertRedoRecord {
    private long scn;
    private long blk;
    private long offset;
    private long seq;
    private int conUid;
    private Xid xid;
    int[] beforeCols;
    int[] afterCols;
    int[] otherCols;
    int objId;
    List<byte[]> after = new ArrayList<>();
    List<byte[]> before = new ArrayList<>();
    List<byte[]> other = new ArrayList<>();
    ChangeCode changeCode;
    String ddlSql;

}
