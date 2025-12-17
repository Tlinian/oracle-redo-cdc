package com.xdream.redo.parser;

import com.example.redo.model.*;
import com.xdream.redo.model.BlockHeader;
import com.xdream.redo.model.ChangeCode;
import com.xdream.redo.model.origin.ChangeHeader;
import com.xdream.redo.model.origin.RedoChange;
import com.xdream.redo.model.origin.RedoRecord;
import com.xdream.redo.util.BinaryUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
        int conUid = BinaryUtil.getU32(recordBytes,16);
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
            return parseRedoChanges(recordBytes,header,length,headerLength, vld,scn,subScn,conUid);
        }else {
            return new RedoRecord(header.blockNumber(),header.sequence(),header.offset(),
                    length,headerLength, vld,scn,subScn,conUid,new ArrayList<>(),null);
        }
    }

    public static RedoRecord parseRedoChanges(byte[] recordBytes,
                                              BlockHeader header,int length,int headerLength, int vld, long scn, int subScn, int conUid) throws SQLException {
        int offset = headerLength;
        List<RedoChange> changes = new ArrayList<>();
        RedoChange change = null;
        while (offset < recordBytes.length) {
            byte layer = recordBytes[offset];
            byte code = recordBytes[offset+1];
            short opcode = (short) (Byte.toUnsignedInt(layer) <<8 | Byte.toUnsignedInt(code));
            switch (ChangeCode.getChangeCode(opcode)) {
                case INSERT ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.INSERT);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                }
                case DELETE ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.DELETE);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                }
                case UPDATE ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UPDATE);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                }
                case INSERT_MULTI ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.INSERT_MULTI);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                }
                case UPDATE_MULTI ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UPDATE_MULTI);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
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
                    change = redoChange;
                }
                case LOB_REDO -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.LOB_REDO);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                } case LOB_KDLIRBIMG -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.LOB_KDLIRBIMG);
                    int[][] vectors = redoChange.getVectors();
                    if (vectors.length > 4){
                        if (recordBytes[vectors[4][1]] == 9){
                            redoChange.setDataObjectId(BinaryUtil.getU32(recordBytes, vectors[4][1]+0x0C));
                        }
                    }
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                }   case LOB_UINDO_REDO -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.LOB_UINDO_REDO);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                }case LOAD_LOB -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.LOAD_LOB);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                }case LLB -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.LLB);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                    change = redoChange;
                }
                case DDL ->{
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.DDL);
                    changes.add(redoChange);
                    change = redoChange;
                    offset += redoChange.changeLength();
                }
                case UNKNOWN -> {
                    RedoChange redoChange = parseRedoChange(recordBytes, offset,ChangeCode.UNKNOWN);
                    changes.add(redoChange);
                    offset += redoChange.changeLength();
                }
            }
        }
        return new RedoRecord(header.blockNumber(),header.sequence(),header.offset(),
                length,headerLength, vld,scn,subScn,conUid,changes,change);
    }

    private static RedoChange parseRedoChange(byte[] recordBytes,  int offset,ChangeCode changeCode) {
        ChangeHeader changeHeader = ChangeHeader.parseChangeHeader(recordBytes, offset);
        int data_object_id = (changeHeader.obj0() << 16) | changeHeader.obj1();
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
        int obj = 0;
        if (vectorSize > 2) {
            int i = vectors[2][1];
            if (i<recordBytes.length-0x24) {
                obj  = BinaryUtil.getU32(recordBytes, i + 0x24);
            }

        }
        return new RedoChange(changeHeader,data_object_id == 0?obj:data_object_id,vectors,vectorCurrent-offset,changeCode);
    }

    private static short appendFour(short data){
        return (short)((data + 3) / 4 * 4);
    }

    private static int appendFour(int data){
        return (data + 3) / 4 * 4;
    }
}
