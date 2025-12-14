package com.example.redo;

import com.example.redo.model.ChangeCode;
import com.example.redo.model.Xid;
import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
@Builder
@Setter
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
    List<byte[]> after;
    List<byte[]> before;
    List<byte[]> other;
    protected ChangeCode changeCode;
    String ddlSql;

    List<List<byte[]>> datas;

    public ConvertRedoRecord() {
    }

    public ConvertRedoRecord(long scn, long blk, long offset, long seq, int conUid, Xid xid, int[] beforeCols, int[] afterCols, int[] otherCols, int objId, List<byte[]> after, List<byte[]> before, List<byte[]> other, ChangeCode changeCode, String ddlSql, List<List<byte[]>> datas) {
        this.scn = scn;
        this.blk = blk;
        this.offset = offset;
        this.seq = seq;
        this.conUid = conUid;
        this.xid = xid;
        this.beforeCols = beforeCols;
        this.afterCols = afterCols;
        this.otherCols = otherCols;
        this.objId = objId;
        this.after = after;
        this.before = before;
        this.other = other;
        this.changeCode = changeCode;
        this.ddlSql = ddlSql;
        this.datas = datas;
    }
}
