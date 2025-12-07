package com.example.redo.model;

import com.example.redo.util.BinaryUtil;

import java.nio.ByteBuffer;

public record ChangeHeader(int layerNumber,
                           int code,
                           int cls,
                           int afn,
                           int obj0,
                           long dba,
                           int seq,
                           int typ,
                           int obj1
                             ) {

    // 12C以上为32， 以下为24
    public static final int CHANGE_HEADER_SIZE = 32;

    public static ChangeHeader parseChangeHeader(byte [] data,int offset) {
        int layer = Byte.toUnsignedInt(data[offset]);
        int code = Byte.toUnsignedInt(data[offset+1]);
        int cls = BinaryUtil.getU16(data, offset+2);
        int afn = BinaryUtil.getU16(data, offset+4);
        int obj0 = BinaryUtil.getU16(data, offset+6);
        int dba = BinaryUtil.getU32(data, offset+8);
        int seq = Byte.toUnsignedInt(data[offset+20]);
        int typ = Byte.toUnsignedInt(data[offset+21]);
        int obj1 = BinaryUtil.getU16(data, offset+22);
        return new ChangeHeader(layer, code, cls, afn, obj0, dba, seq, typ, obj1);
    }

}
