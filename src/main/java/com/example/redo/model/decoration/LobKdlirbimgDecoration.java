package com.example.redo.model.decoration;

import com.example.redo.deserialize.RBA;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.Xid;
import com.example.redo.model.origin.RedoChange;
import com.example.redo.model.origin.RedoRecord;
import com.example.redo.util.BinaryUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@ToString
@Builder
public class LobKdlirbimgDecoration implements RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int objId;
    int colId;
    byte[] lobData;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.LOB_KDLIRBIMG;
    }

    public static LobKdlirbimgDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange redoChange = null;
        for(RedoChange change : record.changes()){
            if (change.changeCode() == ChangeCode.LOB_KDLIRBIMG){
                redoChange = change;
            }
        }
        int objId = redoChange.data_object_id();
        // 73291
        int[][] vectors = redoChange.vectors();
        // xid
        int xid0 = BinaryUtil.getU16(recordBytes,vectors[1][1] + 16);
        int xid1 = BinaryUtil.getU16(recordBytes,vectors[1][1] + 18);
        int xid2 = BinaryUtil.getU16(recordBytes,vectors[1][1] + 20);
        int xid3 = BinaryUtil.getU16(recordBytes,vectors[1][1] + 22);
        Xid xid = new Xid(xid0,xid1,xid2,xid3);
        int colId = BinaryUtil.getU16(recordBytes,vectors[4][1] + 0x12);
        byte[] lobData = Arrays.copyOfRange(recordBytes,vectors[3][1],vectors[3][0]+vectors[3][1]);

        return LobKdlirbimgDecoration.builder()
                .scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .xid(xid)
                .objId(objId)
                .colId(colId)
                .lobData(lobData)
                .build();
    }
}
