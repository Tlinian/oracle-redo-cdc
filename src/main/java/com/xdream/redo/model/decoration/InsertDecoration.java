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
public class InsertDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] afterCols;
    int objId;
    List<byte[]> after;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.INSERT;
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

    public static InsertDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange redoChange = null;
        RedoChange undoChange = null;
        for(RedoChange change : record.changes()){
            if (change.changeCode() == ChangeCode.INSERT){
                redoChange = change;
            }else if (change.changeCode() == ChangeCode.UNDO_BEFORE){
                undoChange = change;
            }
        }
        int objId = redoChange.getDataObjectId();
        // 73291
        int[][] vectors = redoChange.getVectors();
        // xid
        Xid xid = getXid(vectors,recordBytes,undoChange);
        int vectorLength = vectors[2][0];
        int vectorCurrent = vectors[2][1];
        byte[] segment = new byte[vectorLength];
        System.arraycopy(recordBytes,vectorCurrent,segment,0,vectorLength);
        int colCount = Byte.toUnsignedInt(recordBytes[vectors[1][1] + 0x12]);
        int [] cols = new int[colCount];
        for (int i = 0; i < colCount; i++) {
            cols[i] = i;
        }
        List<byte[]> datas = new ArrayList<>();
        // 接下来是字段值的长度
        for (int i = 2; i < 2 + colCount; i++) {
            int len = vectors[i][0];
            int start = vectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            datas.add(data);
        }
        return InsertDecoration.builder()
                .scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .xid(xid)
                .afterCols(cols)
                .objId(objId)
                .after(datas)
                .build();
    }
}
