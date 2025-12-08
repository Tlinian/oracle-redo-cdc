package com.example.redo.parser;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.RedoChange;
import com.example.redo.model.RedoRecord;
import com.example.redo.model.Xid;
import com.example.redo.util.BinaryUtil;
import oracle.sql.NUMBER;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordConvertor {
    public static ConvertRedoRecord convert(RedoRecord record, byte[] recordBytes) throws SQLException {
        List<RedoChange> changes = record.changes();
        ChangeCode changeCode = null;
        for(RedoChange change : changes){
            if (change.changeCode() == ChangeCode.INSERT){
                changeCode = change.changeCode();
            }else if (change.changeCode() == ChangeCode.DELETE){
                changeCode = change.changeCode();
            }else  if (change.changeCode() == ChangeCode.UPDATE){
                changeCode = change.changeCode();
            }else if (change.changeCode() == ChangeCode.DDL){
                changeCode = change.changeCode();
            }else if (change.changeCode() == ChangeCode.COMMIT){
                changeCode = change.changeCode();
            }
        }
        if (changeCode == null){
            return null;
        }
        if (changeCode == ChangeCode.INSERT){
            return insert(record,recordBytes);
        }else if (changeCode == ChangeCode.DELETE){
            return delete(record,recordBytes);
        }else if (changeCode == ChangeCode.UPDATE){
            return update(record,recordBytes);
        }else if (changeCode == ChangeCode.DDL){
            return ddl(record,recordBytes);
        }else if (changeCode == ChangeCode.COMMIT){
            return commit(record,recordBytes);
        }



        return null;
    }

    private static ConvertRedoRecord commit(RedoRecord record, byte[] recordBytes) {
        RedoChange ddlChange = null;
        for (RedoChange change : record.changes()) {
            if (change.changeCode().equals(ChangeCode.COMMIT)){
                ddlChange = change;
            }
        }
        if (ddlChange == null){
            return null;
        }
        int[][] afterVectors = ddlChange.vectors();
        int xid1 = BinaryUtil.getU16(recordBytes,afterVectors[0][1]);
        int cls = ddlChange.changeHeader().cls();
        int xid2 = BinaryUtil.getU32(recordBytes,afterVectors[0][1]+4);
        final short xid0 = (short) (cls >= 0x0F ? (cls - 0x0F) / 2 : -1);
        Xid xid = new Xid(xid0,(int)xid1,(int)xid2);

        return new ConvertRedoRecord(record.scn(), record.blockNumber(), record.offset(), record.sequence(),
                xid, null, null, null, 0,null,null,null, ddlChange.changeCode(), null);
    }

    private static ConvertRedoRecord ddl(RedoRecord record, byte[] recordBytes) {
        RedoChange ddlChange = null;
        for (RedoChange change : record.changes()) {
            if (change.changeCode().equals(ChangeCode.DDL)){
                ddlChange = change;
            }
        }
        if (ddlChange == null){
            return null;
        }

        int[][] vectors = ddlChange.vectors();
        int objId = BinaryUtil.getU32(recordBytes,vectors[0x0B][1]);
        String ddlSql = new String(Arrays.copyOfRange(recordBytes, vectors[7][1], vectors[7][1] + vectors[7][0]-1));
        return new ConvertRedoRecord(record.scn(), record.blockNumber(), record.offset(), record.sequence(),
                null, null, null, null, objId,null,null,null, ddlChange.changeCode(), ddlSql);
    }

    private static Xid getXid(int[][] vectors, byte[] recordBytes){
        int len = vectors[0][0];
        int start = vectors[0][1];
        byte[] segment = new byte[len];
        System.arraycopy(recordBytes,start,segment,0,len);
        int xid0 = BinaryUtil.getU16(segment,8);
        int xid1 = BinaryUtil.getU16(segment,10);
        int xid2 = BinaryUtil.getU32(segment,12);
        return new Xid(xid0,xid1,xid2);
    }

    private static ConvertRedoRecord update(RedoRecord record, byte[] recordBytes) {
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
            afterCols[i/2] = (Byte.toUnsignedInt(afterColsBytes[i+1]) << 8) | Byte.toUnsignedInt(afterColsBytes[i]);
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
            beforeCols[i/2] = (Byte.toUnsignedInt(beforeColsBytes[i+1]) << 8) | Byte.toUnsignedInt(beforeColsBytes[i]);
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
            otherCols[i/2] = (Byte.toUnsignedInt(otherColsBytes[i+1]) << 8) | Byte.toUnsignedInt(otherColsBytes[i])-1;
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
//        System.out.println("--------------------");
//        System.out.println("obj Id: "+objId);
//        System.out.println(" op:"+redoChange.changeCode());
//        System.out.println("colIds:"+ Arrays.toString(cols));
//        System.out.println("data:"+ Arrays.toString(datas.toArray()));

        return new ConvertRedoRecord(
                record.scn(),
                record.blockNumber(),
                record.offset(),
                record.sequence(),
                xid,
                beforeCols,
                afterCols,
                otherCols,
                objId,
                afterDatas,beforeDatas,otherDatas,updateChange.changeCode(),null
        );
    }

    private static ConvertRedoRecord delete(RedoRecord record, byte[] recordBytes) {
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
//        System.out.println("--------------------");
//        System.out.println("obj Id: "+objId);
//        System.out.println(" op:"+redoChange.changeCode());
//        System.out.println("colIds:"+ Arrays.toString(cols));
//        System.out.println("data:"+ Arrays.toString(datas.toArray()));

        return new ConvertRedoRecord(
                record.scn(),
                record.blockNumber(),
                record.offset(),
                record.sequence(),
                beforeXid,
                beforeCols,
                null,
                null,
                objId,
                null,beforeDatas,null,updateChange.changeCode(),null
        );
    }

    private static ConvertRedoRecord insert(RedoRecord record, byte[] recordBytes) {
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
            cols[i] = i+1;
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
//        System.out.println("--------------------");
//        System.out.println("obj Id: "+objId);
//        System.out.println(" op:"+redoChange.changeCode());
//        System.out.println("colIds:"+ Arrays.toString(cols));
//        System.out.println("data:"+ Arrays.toString(datas.toArray()));

        return new ConvertRedoRecord(
                record.scn(),
                record.blockNumber(),
                record.offset(),
                record.sequence(),
                xid,
                null,
                cols,
                null,
                objId,
                datas,null,null,redoChange.changeCode(),null
        );
    }
}
