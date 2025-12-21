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
import java.util.Arrays;
import java.util.List;

@Getter
@Builder
public class DeleteMultiDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] beforeCols;
    int objId;
    List<List<byte[]>> datas;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.DELETE_MULTI;
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


    public static DeleteMultiDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange redoChange = null;
        RedoChange undoChange = null;
        for(RedoChange change : record.changes()){
            if (change.changeCode() == ChangeCode.DELETE_MULTI){
                redoChange = change;
            }else if (change.changeCode() == ChangeCode.UNDO_BEFORE){
                undoChange = change;
            }
        }
        int objId = redoChange.getDataObjectId();
        // 73291
        int[][] vectors = undoChange.getVectors();
        // xid
        Xid xid = getXid(vectors,recordBytes);
        int rowCount = Byte.toUnsignedInt(recordBytes[vectors[3][1] + 0x12]);
        int startPosition = vectors[5][1];
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
                if (len == 0xFE){
                    len = Short.toUnsignedInt(BinaryUtil.getU16(recordBytes,startPosition));
                    startPosition+=2;
                }else if (len == 0xFF){
                    len=0;
                }
                int start = startPosition;
                byte[] value = Arrays.copyOfRange(recordBytes, start, start+len);
                data.add(value);
                startPosition+=len;
            }
            cols.add(col);
            datas.add(data);
        }
        return DeleteMultiDecoration.builder()
                .scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .xid(xid)
                .beforeCols(cols.get(0).stream().mapToInt(Integer::intValue).toArray())
                .objId(objId)
                .datas(datas)
                .build();
    }
}
