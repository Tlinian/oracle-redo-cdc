package com.example.redo.model.decoration;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.deserialize.RBA;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.Xid;
import com.example.redo.model.origin.RedoChange;
import com.example.redo.model.origin.RedoRecord;
import com.example.redo.util.BinaryUtil;
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

    private static Xid getXid(int[][] vectors, byte[] recordBytes){
        int len = vectors[0][0];
        int start = vectors[0][1];
        byte[] segment = new byte[len];
        System.arraycopy(recordBytes,start,segment,0,len);
        int xid0 = BinaryUtil.getU16(segment,8);
        int xid1 = BinaryUtil.getU16(segment,10);
        int xid2 = BinaryUtil.getU16(segment,12);
        int xid3 = BinaryUtil.getU16(segment,14);
        return new Xid(xid0,xid1,xid2,xid3);
    }

    public static InsertDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange redoChange = null;
        for(RedoChange change : record.changes()){
            if (change.changeCode() == ChangeCode.INSERT){
                redoChange = change;
            }
        }
        int objId = redoChange.data_object_id();
        // 73291
        int[][] vectors = redoChange.vectors();
        // xid
        Xid xid = getXid(vectors,recordBytes);

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
