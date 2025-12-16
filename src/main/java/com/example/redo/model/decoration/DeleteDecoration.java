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
        int objId = updateChange.data_object_id();

        // after data start with 3
        int beforeStartIndex = 3;
        int[][] beforeVectors = undoChange.vectors();

        Xid beforeXid = getXid(beforeVectors, recordBytes);
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
