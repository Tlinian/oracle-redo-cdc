package com.xdream.redo.model.decoration;

import com.xdream.redo.deserialize.RBA;
import com.xdream.redo.model.ChangeCode;
import com.xdream.redo.model.Xid;
import com.xdream.redo.model.origin.RedoChange;
import com.xdream.redo.model.origin.RedoRecord;
import com.xdream.redo.util.BinaryUtil;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class CommitDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] beforeCols;
    int objId;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.COMMIT;
    }

    public static CommitDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange ddlChange = null;
        for (RedoChange change : record.changes()) {
            if (change.changeCode().equals(ChangeCode.COMMIT)){
                ddlChange = change;
            }
        }
        if (ddlChange == null){
            return null;
        }
        int[][] afterVectors = ddlChange.getVectors();
        int xid1 = BinaryUtil.getU16(recordBytes,afterVectors[0][1]);
        int cls = ddlChange.changeHeader().cls();
        int xid2 = BinaryUtil.getU16(recordBytes,afterVectors[0][1]+4);
        int xid3 = BinaryUtil.getU16(recordBytes,afterVectors[0][1]+6);
        final short xid0 = (short) (cls >= 0x0F ? (cls - 0x0F) / 2 : -1);
        Xid xid = new Xid(xid0, xid1, xid2, xid3);
        return CommitDecoration.builder()
                .scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .xid(xid)
                .build();
    }
}
