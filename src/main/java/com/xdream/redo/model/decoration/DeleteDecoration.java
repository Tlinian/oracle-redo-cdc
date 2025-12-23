package com.xdream.redo.model.decoration;

import com.xdream.redo.deserialize.RBA;
import com.xdream.redo.model.ChangeCode;
import com.xdream.redo.model.Xid;
import com.xdream.redo.model.origin.RedoChange;
import com.xdream.redo.model.origin.RedoRecord;
import com.xdream.redo.util.BinaryUtil;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Builder
public class DeleteDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] beforeCols;
    int objId;
    List<byte[]> before;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.DELETE;
    }

    private static Xid getXid(int[][] vectors, byte[] recordBytes,RedoChange undoChange){
        if (Byte.toUnsignedInt(recordBytes[vectors[0][1]]) == 0x01 ||
                Byte.toUnsignedInt(recordBytes[vectors[0][1]]) == 0x11) {
            final int start = (Byte.toUnsignedInt(recordBytes[vectors[0][1] + 1]) & 0x08) == 0 ?
                    4 : 8;
            return new Xid(
                    BinaryUtil.getU16(recordBytes, vectors[0][1] + start),
                    BinaryUtil.getU16(recordBytes, vectors[0][1] + start + 0x02),
                    BinaryUtil.getU16(recordBytes, vectors[0][1] + start + 0x04),
                    BinaryUtil.getU16(recordBytes, vectors[0][1] + start + 0x06));
        }
        int[][] coords = undoChange.getVectors();
        return new Xid(
                BinaryUtil.getU16(recordBytes, coords[0][1] + 0x08),
                BinaryUtil.getU16(recordBytes, coords[0][1] + 0x0A),
                BinaryUtil.getU16(recordBytes, coords[0][1] + 0x0C),
                BinaryUtil.getU16(recordBytes, coords[0][1] + 0x0E));
    }

    public static DeleteDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange updateChange = null;
        RedoChange undoChange = null;
        for (RedoChange change : record.changes()) {
            if (change.changeCode().equals(ChangeCode.DELETE)){
                updateChange = change;
            }else if (change.changeCode().equals(ChangeCode.UNDO_BEFORE)){
                undoChange = change;
            }
        }
        int objId = updateChange.getDataObjectId();
        if (undoChange == null) {
            // rollback
            return null;
        }
        // after data start with 3
        int beforeStartIndex = 3;
        int[][] beforeVectors = undoChange.getVectors();

        Xid beforeXid = getXid(beforeVectors, recordBytes,undoChange);
        int beforeColCount = Byte.toUnsignedInt(recordBytes[beforeVectors[beforeStartIndex][1] + 0x12]);
        int[] beforeCols = new int[beforeColCount];
        for (int i = 0; i < beforeColCount; i++) {
            beforeCols[i] = i;
        }

        List<byte[]> beforeDatas = new ArrayList<>();
//        // 接下来是字段值的长度
        int beforeStartDataIndex = beforeStartIndex+1;
        for (int i = beforeStartDataIndex; i < beforeStartDataIndex + beforeColCount; i++) {
            int len = beforeVectors[i][0];
            int start = beforeVectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            beforeDatas.add(data);
        }
        return DeleteDecoration.builder()
                .scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .xid(beforeXid)
                .beforeCols(beforeCols)
                .objId(objId)
                .before(beforeDatas)
                .build();
    }
}
