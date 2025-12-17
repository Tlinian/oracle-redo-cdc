package com.example.redo.model.decoration;

import com.example.redo.deserialize.RBA;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.Xid;
import com.example.redo.model.origin.RedoChange;
import com.example.redo.model.origin.RedoRecord;
import com.example.redo.util.BinaryUtil;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class LlbDecoration implements  RecordDecoration {
    private int obj;
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    private int columnId;
    private int lSize;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.LLB;
    }

    public static RecordDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange ddlChange =  record.change();
        if (ddlChange == null){
            return null;
        }
        int[][] afterVectors = ddlChange.getVectors();
        int xid0 =  BinaryUtil.getU16(recordBytes,afterVectors[2][1]+4);
        int xid1 =BinaryUtil.getU16(recordBytes,afterVectors[2][1]+6);
        int xid2 = BinaryUtil.getU16(recordBytes,afterVectors[2][1]+8);
        int xid3 = BinaryUtil.getU16(recordBytes,afterVectors[2][1]+10);
        Xid xid = new Xid(xid0, xid1, xid2, xid3);
        int colId = BinaryUtil.getU16(recordBytes, afterVectors[2][1] + 0x16);
        int lSize = BinaryUtil.getU32(recordBytes, afterVectors[2][1] + 0x20);
        return LlbDecoration.builder().columnId(colId).lSize(lSize)
                .obj(ddlChange.getDataObjectId()).scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid()).xid(xid).build();
    }
}
