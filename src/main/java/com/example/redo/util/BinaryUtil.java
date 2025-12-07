package com.example.redo.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public final class BinaryUtil {
    private BinaryUtil() {}

    public static final ByteOrder LITTLE_ENDIAN = ByteOrder.LITTLE_ENDIAN;

    public static ByteBuffer allocateLE(int size) {
        return ByteBuffer.allocate(size).order(LITTLE_ENDIAN);
    }

    public static void readFully(FileChannel channel, long position, ByteBuffer buffer) throws IOException {
        buffer.clear();
        int read = 0;
        while (buffer.hasRemaining() && read != -1) {
            int n = channel.read(buffer, position + read);
            if (n < 0) {
                break;
            }
            read += n;
        }
        buffer.flip();
    }

    public static int getU32(byte[] buffer, int offset) {
        return Byte.toUnsignedInt(buffer[offset])
                | Byte.toUnsignedInt(buffer[offset + 1]) << 8
                | Byte.toUnsignedInt(buffer[offset + 2]) << 16
                | Byte.toUnsignedInt(buffer[offset + 3]) << 24;
    }

    public static short getU16(byte[] buffer, int offset) {
        return (short)(Byte.toUnsignedInt(buffer[offset]) | Byte.toUnsignedInt(buffer[offset + 1]) << 8);
    }

    public static short getU16Special(byte[] buffer, int offset) {
        return (short)(Byte.toUnsignedInt(buffer[offset])
                | Byte.toUnsignedInt((byte)(buffer[offset + 1] & 127)) << 8);
    }
}

