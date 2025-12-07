package com.example.redo.model;

import com.example.redo.util.BinaryUtil;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public record BlockHeader(int signature, long blockNumber, long sequence, int offset, int checksum) {

    public static final int BLOCK_HEADER_SIZE = 16;

    public static BlockHeader parseBlockHeader(byte[] block) {
        ByteBuffer buf = ByteBuffer.wrap(block).order(LITTLE_ENDIAN);
        int signature = buf.getInt();
        long blockNumber = Integer.toUnsignedLong(buf.getInt());
        long sequence = Integer.toUnsignedLong(buf.getInt());
        short offset = BinaryUtil.getU16Special(block, buf.position());
        short checksum = BinaryUtil.getU16(block, buf.position() + 2);
        return new BlockHeader(signature, blockNumber, sequence, offset, checksum);
    }
}
