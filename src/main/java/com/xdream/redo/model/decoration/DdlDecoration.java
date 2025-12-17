package com.xdream.redo.model.decoration;

import com.xdream.redo.deserialize.RBA;
import com.xdream.redo.model.ChangeCode;
import com.xdream.redo.model.Xid;
import com.xdream.redo.model.origin.RedoChange;
import com.xdream.redo.model.origin.RedoRecord;
import com.xdream.redo.util.BinaryUtil;
import lombok.Builder;
import lombok.Getter;

import java.util.Arrays;

@Getter
@Builder
public class DdlDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] beforeCols;
    int objId;
    String sql;
    int kind;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.DDL;
    }

    public static DdlDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange ddlChange = null;
        for (RedoChange change : record.changes()) {
            if (change.changeCode().equals(ChangeCode.DDL)){
                ddlChange = change;
            }
        }
        if (ddlChange == null){
            return null;
        }
        int[][] vectors = ddlChange.getVectors();
        Xid xid = new Xid(
                BinaryUtil.getU16(recordBytes, vectors[0][1] + 0x04),
                BinaryUtil.getU16(recordBytes, vectors[0][1] + 0x06),
                BinaryUtil.getU16(recordBytes, vectors[0][1] + 0x08),
                BinaryUtil.getU16(recordBytes, vectors[0][1] + 0x0A));
        int kind = BinaryUtil.getU16(recordBytes, vectors[0][1] + 0x10);
        if (kind == 0x04 || kind == 0x05 || kind == 0x06 ||
                kind == 0x08 || kind == 0x09 || kind == 0x0A){
            return null;
        }
        int objId = BinaryUtil.getU32(recordBytes,vectors[0x0B][1]);
        String ddlSql = new String(Arrays.copyOfRange(recordBytes, vectors[7][1], vectors[7][1] + vectors[7][0]-1));
        return DdlDecoration.builder().scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .objId(objId)
                .sql(ddlSql)
                .kind(kind)
                .xid(xid)
                .build();
    }
}
