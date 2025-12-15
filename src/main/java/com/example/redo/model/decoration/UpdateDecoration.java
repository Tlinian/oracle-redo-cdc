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
public class UpdateDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] beforeCols;
    int[] afterCols;
    int[] otherCols;
    int objId;
    List<byte[]> after;
    List<byte[]> before;
    List<byte[]> other;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.UPDATE;
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

    public static UpdateDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange updateChange = null;
        RedoChange undoChange = null;
        for (RedoChange change : record.changes()) {
            if (change.changeCode().equals(ChangeCode.UPDATE)){
                updateChange = change;
            }else if (change.changeCode().equals(ChangeCode.UNDO_BEFORE)){
                undoChange = change;
            }
        }

        int objId = updateChange.data_object_id();
        // after data start with 2
        int[][] afterVectors = updateChange.vectors();

        Xid xid = getXid(afterVectors, recordBytes);
        int vectorLength = afterVectors[2][0];
        int vectorCurrent = afterVectors[2][1];
        byte[] afterColsBytes = new byte[vectorLength];
        System.arraycopy(recordBytes,vectorCurrent,afterColsBytes,0,vectorLength);
        int colCount = afterColsBytes.length/2;
        int [] afterCols = new int[colCount];
        for (int i = 0; i < afterColsBytes.length; i+=2) {
            afterCols[i/2] = (Byte.toUnsignedInt(afterColsBytes[i+1]) << 8) | Byte.toUnsignedInt(afterColsBytes[i])+1;
        }
        List<byte[]> afterDatas = new ArrayList<>();
        int afterStartIndex = 3;
        // 接下来是字段值的长度
        for (int i = afterStartIndex; i < afterStartIndex + colCount; i++) {
            int len = afterVectors[i][0];
            int start = afterVectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            afterDatas.add(data);
        }

        // before data start with 4, 镜像数据
        int beforeStartIndex = 4;
        int[][] beforeVectors = undoChange.vectors();
        int vectorLengthBefore = beforeVectors[beforeStartIndex][0];
        int vectorCurrentBefore = beforeVectors[beforeStartIndex][1];
        byte[] beforeColsBytes = new byte[vectorLengthBefore];
        System.arraycopy(recordBytes,vectorCurrentBefore,beforeColsBytes,0,vectorLengthBefore);
        int colCountBefore = beforeColsBytes.length/2;
        int [] beforeCols = new int[colCountBefore];
        for (int i = 0; i < beforeColsBytes.length; i+=2) {
            beforeCols[i/2] = (Byte.toUnsignedInt(beforeColsBytes[i+1]) << 8) | Byte.toUnsignedInt(beforeColsBytes[i])+1;
        }
        List<byte[]> beforeDatas = new ArrayList<>();
        int beforeStartDataIndex = beforeStartIndex+1;
        // 接下来是字段值的长度
        for (int i = beforeStartDataIndex; i < beforeStartDataIndex + colCountBefore; i++) {
            int len = beforeVectors[i][0];
            int start = beforeVectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            beforeDatas.add(data);
        }

        // before data start with , 其他列数据，其中一个向量不知道是是啥。
        int beforeOtherStartIndex = beforeStartDataIndex + colCountBefore+1;

        int vectorLengthOther = beforeVectors[beforeOtherStartIndex][0];
        int vectorCurrentOther = beforeVectors[beforeOtherStartIndex][1];
        byte[] otherColsBytes = new byte[vectorLengthOther];
        System.arraycopy(recordBytes,vectorCurrentOther,otherColsBytes,0,vectorLengthOther);
        int colCountOther = otherColsBytes.length/2;
        int [] otherCols = new int[colCountOther];
        // 此处列索引要-1
        for (int i = 0; i < otherColsBytes.length; i+=2) {
            otherCols[i/2] = (Byte.toUnsignedInt(otherColsBytes[i+1]) << 8) | Byte.toUnsignedInt(otherColsBytes[i]);
        }

        List<byte[]> otherDatas = new ArrayList<>();
        // 向量要间隔1
        int otherStartDataIndex = beforeOtherStartIndex+2;
        // 接下来是字段值的长度
        for (int i = otherStartDataIndex; i < otherStartDataIndex + colCountOther; i++) {
            int len = beforeVectors[i][0];
            int start = beforeVectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            otherDatas.add(data);
        }
        return UpdateDecoration.builder()
                .scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .xid(xid)
                .beforeCols(beforeCols)
                .afterCols(afterCols)
                .otherCols(otherCols)
                .objId(objId)
                .after(afterDatas)
                .before(beforeDatas)
                .other(otherDatas)
                .build();
    }
}
