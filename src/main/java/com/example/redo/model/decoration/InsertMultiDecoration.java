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
import java.util.Arrays;
import java.util.List;

@Getter
@Builder
public class InsertMultiDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] afterCols;
    int objId;
    List<List<byte[]>> datas;

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


    public static InsertMultiDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange redoChange = null;
        for(RedoChange change : record.changes()){
            if (change.changeCode() == ChangeCode.INSERT_MULTI){
                redoChange = change;
            }
        }
        int objId = redoChange.data_object_id();
        // 73291
        int[][] vectors = redoChange.vectors();
        // xid
        Xid xid = getXid(vectors,recordBytes);
        int rowCount = Byte.toUnsignedInt(recordBytes[vectors[1][1] + 0x12]);
        int startPosition = vectors[3][1];
        List<List<byte[]>> datas = new ArrayList<>();
        List<List<Integer>> cols = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            startPosition+=2;
            int colCount = Byte.toUnsignedInt(recordBytes[startPosition]);
            startPosition++;
            List<byte[]> data = new ArrayList<>();
            List<Integer> col = new ArrayList<>();
            for (int j = 0; j < colCount; j++) {
                col.add(j);
                int len = Byte.toUnsignedInt(recordBytes[startPosition ]);
                startPosition++;
                int start = startPosition;
                byte[] value = Arrays.copyOfRange(recordBytes, start, start+len);
                data.add(value);
                startPosition+=len;
            }
            cols.add(col);
            datas.add(data);
        }
        return InsertMultiDecoration.builder()
                .scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .xid(xid)
                .afterCols(cols.get(0).stream().mapToInt(Integer::intValue).toArray())
                .objId(objId)
                .datas(datas)
                .build();
    }
}
