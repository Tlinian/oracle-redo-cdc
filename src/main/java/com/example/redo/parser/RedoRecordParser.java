package com.example.redo.parser;

import com.example.redo.model.*;
import com.example.redo.util.BinaryUtil;
import com.fasterxml.jackson.databind.DeserializationConfig;
import oracle.sql.NUMBER;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RedoRecordParser {

    public static long getScn4Record(byte[] recordBytes, int offset) {
        if (recordBytes[offset] == -1 && recordBytes[offset + 1] == -1 && recordBytes[offset + 2] == -1 && recordBytes[offset + 3] == -1 && recordBytes[offset + 4] == -1 && recordBytes[offset + 5] == -1) {
            return Long.MAX_VALUE;
        } else {
            return (recordBytes[offset + 1] & 128) == 128 ? Byte.toUnsignedLong(recordBytes[offset + 2])
                    | Byte.toUnsignedLong(recordBytes[offset + 3]) << 8 | Byte.toUnsignedLong(recordBytes[offset + 4]) << 16
                    | Byte.toUnsignedLong(recordBytes[offset + 5]) << 24 | Byte.toUnsignedLong(recordBytes[offset]) << 48
                    | Byte.toUnsignedLong((byte)(recordBytes[offset + 1] & 127)) << 56 : Byte.toUnsignedLong(recordBytes[offset + 2])
                    | Byte.toUnsignedLong(recordBytes[offset + 3]) << 8 | Byte.toUnsignedLong(recordBytes[offset + 4]) << 16
                    | Byte.toUnsignedLong(recordBytes[offset + 5]) << 24 | Byte.toUnsignedLong(recordBytes[offset + 0]) << 32
                    | Byte.toUnsignedLong(recordBytes[offset + 1]) << 40;
        }
    }

    public static RedoRecord parseRedoRecord(BlockHeader header, byte[] recordBytes) throws SQLException {
        // 4
        int length = BinaryUtil.getU32(recordBytes,0);
        int vld = Byte.toUnsignedInt(recordBytes[4]);
        // 这里scn可能是commitScn
        long scn = getScn4Record(recordBytes,6);
        int subScn = BinaryUtil.getU32(recordBytes,12);
        int headerLength;
        if ((vld&4) ==4){
            headerLength = 68;
        }else {
            headerLength = 24;
        }
        boolean hasChange;
        if ((vld&1) ==1){
            hasChange = true;
        }else {
            hasChange = false;
        }

        if (hasChange) {
            List<RedoChange> changes = parseRedoChanges(headerLength,recordBytes);
            return new RedoRecord(header.blockNumber(),header.sequence(),header.offset(),
                    length,headerLength, vld,scn,subScn,changes);
        }else {
            return new RedoRecord(header.blockNumber(),header.sequence(),header.offset(),
                    length,headerLength, vld,scn,subScn,new ArrayList<>());
        }
    }

    public static List<RedoChange> parseRedoChanges(int headerLength, byte[] recordBytes) throws SQLException {
        int offset = headerLength;
        List<RedoChange> changes = new ArrayList<>();
        ChangeCode changeCode = null;
        while (offset < recordBytes.length) {
            byte layer = recordBytes[offset];
            byte code = recordBytes[offset+1];
            short opcode = (short) (Byte.toUnsignedInt(layer) <<8 | Byte.toUnsignedInt(code));
            switch (ChangeCode.getChangeCode(opcode)) {
                case INSERT ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.INSERT);
                    changes.add(redoChange);
                    printInsert(redoChange,recordBytes);
                    offset += redoChange.changeLength();
                }
                case DELETE ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.DELETE);
                    changes.add(redoChange);
                    changeCode = redoChange.changeCode();
                    offset += redoChange.changeLength();
                }
                case UPDATE ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UPDATE);
                    changes.add(redoChange);
                    changeCode = ChangeCode.UPDATE;
                    offset += redoChange.changeLength();
                }
                case UNDO_SEM ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UNDO_SEM);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                }
                case UNDO_BEFORE -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UNDO_BEFORE);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                }
                case COMMIT -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.COMMIT);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                }
                case DDL ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.DDL);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                }
                case UNKNOWN -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UNKNOWN);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                }
            }
        }

        if (Objects.equals(changeCode, ChangeCode.UPDATE)) {
            printUpdate(changes,recordBytes);
        }

        if (Objects.equals(changeCode, ChangeCode.DELETE)) {
            printDelete(changes,recordBytes);
        }
        return changes;
    }

    private static RedoChange parseRedoChange(byte[] recordBytes,  int offset,ChangeCode changeCode) {
        ChangeHeader changeHeader = ChangeHeader.parseChangeHeader(recordBytes, offset);
        int data_object_id = (changeHeader.obj0() << 16) | changeHeader.obj1();
        if (changeCode.equals(ChangeCode.INSERT)&&(data_object_id == 73291||data_object_id == 73059)) {
            System.out.print("");
        }
        int vectorActTabLength = BinaryUtil.getU16(recordBytes, offset +ChangeHeader.CHANGE_HEADER_SIZE);
        int vectorSize = (vectorActTabLength - 2)/2;
        int vectorStart = offset +ChangeHeader.CHANGE_HEADER_SIZE+2;
        int vectorTabLength = appendFour(vectorActTabLength);
        int vectorCurrent = offset + ChangeHeader.CHANGE_HEADER_SIZE+vectorTabLength;
        int[][] vectors = new int[vectorSize][2];
        for (int i = 0; i < vectorSize; i++) {
            short vectorLength = BinaryUtil.getU16(recordBytes, vectorStart + i * 2);
            // 将vectorLength不到4的倍数，自动凑齐四的倍数

            short actVectorLength = appendFour(vectorLength);
            vectors[i][0] = vectorLength;
            vectors[i][1] = vectorCurrent;
            vectorCurrent+=actVectorLength;
        }
        return new RedoChange(changeHeader,data_object_id,vectors,vectorCurrent-offset,changeCode);
    }

    private static short appendFour(short data){
        return (short)((data + 3) / 4 * 4);
    }

    private static int appendFour(int data){
        return (data + 3) / 4 * 4;
    }

    private static void printInsert(RedoChange redoChange,byte[] recordBytes) throws SQLException {
        int objId = redoChange.data_object_id();
        // 73291
        if (objId == 73291||objId == 73059){
            System.out.print("");
        }else {
            return;
        }
        int[][] vectors = redoChange.vectors();
        int vectorLength = vectors[2][0];
        int vectorCurrent = vectors[2][1];
        byte[] segment = new byte[vectorLength];
        System.arraycopy(recordBytes,vectorCurrent,segment,0,vectorLength);
        int colCount = Byte.toUnsignedInt(recordBytes[vectors[1][1] + 0x12]);
        int [] cols = new int[colCount];
        for (int i = 0; i < colCount; i++) {
            cols[i] = i+1;
        }
        List<Object> datas = new ArrayList<>();
        // 接下来是字段值的长度
        for (int i = 2; i < 2 + colCount; i++) {
            int len = vectors[i][0];
            int start = vectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            if (len == 2){
                datas.add(NUMBER.toBigDecimal(data));
            }else {
                datas.add(new String(data));
            }

        }
        System.out.println("--------------------");
        System.out.println("obj Id: "+objId);
        System.out.println(" op:"+redoChange.changeCode());
        System.out.println("colIds:"+ Arrays.toString(cols));
        System.out.println("data:"+ Arrays.toString(datas.toArray()));
    }

    private static void printUpdate(List<RedoChange> redoChange,byte[] recordBytes) throws SQLException {
        RedoChange updateChange = null;
        RedoChange undoChange = null;
        for (RedoChange change : redoChange) {
            if (change.changeCode().equals(ChangeCode.UPDATE)){
                updateChange = change;
            }else if (change.changeCode().equals(ChangeCode.UNDO_BEFORE)){
                undoChange = change;
            }
        }

        int objId = updateChange.data_object_id();
        // 73291
        if (objId == 73474){
            System.out.print("");
        }else {
            return;
        }

        // after data start with 2
        int[][] afterVectors = updateChange.vectors();
        int vectorLength = afterVectors[2][0];
        int vectorCurrent = afterVectors[2][1];
        byte[] afterColsBytes = new byte[vectorLength];
        System.arraycopy(recordBytes,vectorCurrent,afterColsBytes,0,vectorLength);
        int colCount = afterColsBytes.length/2;
        int [] afterCols = new int[colCount];
        for (int i = 0; i < afterColsBytes.length; i+=2) {
            afterCols[i/2] = (Byte.toUnsignedInt(afterColsBytes[i+1]) << 8) | Byte.toUnsignedInt(afterColsBytes[i]);
        }
        List<Object> afterDatas = new ArrayList<>();
        int afterStartIndex = 3;
        // 接下来是字段值的长度
        for (int i = afterStartIndex; i < afterStartIndex + colCount; i++) {
            int len = afterVectors[i][0];
            int start = afterVectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            if (afterCols[i-afterStartIndex] == 0){
                afterDatas.add(NUMBER.toBigDecimal(data));
            }else {
                afterDatas.add(new String(data));
            }
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
        List<Object> beforeDatas = new ArrayList<>();
        int beforeStartDataIndex = beforeStartIndex+1;
        // 接下来是字段值的长度
        for (int i = beforeStartDataIndex; i < beforeStartDataIndex + colCountBefore; i++) {
            int len = beforeVectors[i][0];
            int start = beforeVectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            if (beforeCols[i-beforeStartDataIndex] == 0){
                beforeDatas.add(NUMBER.toBigDecimal(data));
            }else {
                beforeDatas.add(new String(data));
            }
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

        List<Object> otherDatas = new ArrayList<>();
        // 向量要间隔1
        int otherStartDataIndex = beforeOtherStartIndex+2;
        // 接下来是字段值的长度
        for (int i = otherStartDataIndex; i < otherStartDataIndex + colCountOther; i++) {
            int len = beforeVectors[i][0];
            int start = beforeVectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            if (otherCols[i-otherStartDataIndex] == 0){
                otherDatas.add(NUMBER.toBigDecimal(data));
            }else {
                otherDatas.add(new String(data));
            }
        }
        System.out.println("--------------------");
        System.out.println("obj Id: "+objId);
        System.out.println(" op:"+updateChange.changeCode());
        System.out.println("colIds:"+ Arrays.toString(afterCols));
        System.out.println("data:"+ Arrays.toString(afterDatas.toArray()));
    }

    private static void printDelete(List<RedoChange> redoChange,byte[] recordBytes) throws SQLException {
        RedoChange updateChange = null;
        RedoChange undoChange = null;
        for (RedoChange change : redoChange) {
            if (change.changeCode().equals(ChangeCode.DELETE)){
                updateChange = change;
            }else if (change.changeCode().equals(ChangeCode.UNDO_BEFORE)){
                undoChange = change;
            }
        }

        int objId = updateChange.data_object_id();
        // 73291
        if (objId == 73474){
            System.out.print("");
        }else {
            return;
        }

        // after data start with 3
        int beforeStartIndex = 3;
        int[][] beforeVectors = undoChange.vectors();
        int beforeColCount = Byte.toUnsignedInt(recordBytes[beforeVectors[beforeStartIndex][1] + 0x12]);
        List<Object> beforeDatas = new ArrayList<>();
//        // 接下来是字段值的长度
        int beforeStartDataIndex = beforeStartIndex+1;
        for (int i = beforeStartDataIndex; i < beforeStartDataIndex + beforeColCount; i++) {
            int len = beforeVectors[i][0];
            int start = beforeVectors[i][1];
            byte[] data = new byte[len];
            System.arraycopy(recordBytes,start,data,0,len);
            if (i-beforeStartDataIndex == 0){
                beforeDatas.add(NUMBER.toBigDecimal(data));
            }else {
                beforeDatas.add(new String(data));
            }
        }

        System.out.println("--------------------");
        System.out.println("obj Id: "+objId);
        System.out.println(" op:"+updateChange.changeCode());
//        System.out.println("colIds:"+ Arrays.toString(beforeCols));
//        System.out.println("data:"+ Arrays.toString(beforeDatas.toArray()));
    }
}
