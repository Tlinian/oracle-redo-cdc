package com.example.redo.parser;

import com.example.redo.model.*;
import com.example.redo.util.BinaryUtil;
import oracle.sql.NUMBER;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RedoRecordParser {

    public static RedoRecord parseRedoRecord(BlockHeader header, byte[] recordBytes) throws SQLException {
        // 4
        int length = BinaryUtil.getU32(recordBytes,0);
        int vld = Byte.toUnsignedInt(recordBytes[4]);
        // 这里scn可能是commitScn
        int scn = BinaryUtil.getU32(recordBytes,6);
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
                    length,headerLength, vld,scn,subScn,new ArrayList<>());
        }else {
            return new RedoRecord(header.blockNumber(),header.sequence(),header.offset(),
                    length,headerLength, vld,scn,subScn,new ArrayList<>());
        }
    }

    public static List<RedoChange> parseRedoChanges(int headerLength, byte[] recordBytes) throws SQLException {
        int offset = headerLength;
        List<RedoChange> changes = new ArrayList<>();
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
                    offset += redoChange.changeLength();
                }
                case UPDATE ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UPDATE);
                    changes.add(redoChange);
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
                case UNKNOWN -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UNKNOWN);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                }
            }
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
}
