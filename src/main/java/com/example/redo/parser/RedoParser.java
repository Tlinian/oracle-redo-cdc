package com.example.redo.parser;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.deserialize.Deserializer;
import com.example.redo.deserialize.RecordDeserializer;
import com.example.redo.model.*;
import com.example.redo.util.BinaryUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RedoParser {

    private static final int DEFAULT_BLOCKSIZE = 512;
    private static final int FH0_BLOCKSIZE_OFFSET = 5 * 4; // unknown0[5] 之后
    // 依据 change_struct / KTB_struct 常见 opcode 编码，预置映射；可持续扩充
    private static final Map<String, OperationType> OPCODE_MAP = Map.ofEntries(
            // Layer 4: Block Cleanout
            Map.entry("4.1", OperationType.BLOCK_CLEANOUT),
            // Layer 5: Transaction (KTUXE)
            Map.entry("5.1", OperationType.UNDO),
            Map.entry("5.2", OperationType.BEGIN_TX),
            Map.entry("5.3", OperationType.BEGIN_TX),
            Map.entry("5.4", OperationType.COMMIT_TX),
            Map.entry("5.5", OperationType.ROLLBACK_TX),
            // Layer 9: Data layer meta (常见 DDL 相关)
            Map.entry("9.1", OperationType.DDL),
            Map.entry("9.2", OperationType.DDL),
            // Layer 10: Index (KIX)
            Map.entry("10.1", OperationType.INDEX_OP), // insert key
            Map.entry("10.2", OperationType.INDEX_OP), // delete key
            Map.entry("10.3", OperationType.INDEX_OP), // update key
            // Layer 11: Table / Row (KDO)
            Map.entry("11.1", OperationType.INSERT_ROW),
            Map.entry("11.2", OperationType.INSERT_ROW), // 常见单行插入
            Map.entry("11.3", OperationType.UPDATE_ROW),
            Map.entry("11.4", OperationType.UPDATE_ROW),
            Map.entry("11.5", OperationType.INSERT_ROW), // 部分版本 INSERT piece
            Map.entry("11.6", OperationType.UPDATE_ROW),
            Map.entry("11.9", OperationType.UPDATE_ROW),
            Map.entry("11.10", OperationType.UPDATE_ROW),
            Map.entry("11.11", OperationType.DELETE_ROW),
            Map.entry("11.12", OperationType.DELETE_ROW),
            Map.entry("11.13", OperationType.UPDATE_ROW), // migrate/chain
            // Layer 13/14: Segment / Extent
            Map.entry("13.1", OperationType.SEGMENT_OP),
            Map.entry("13.2", OperationType.SEGMENT_OP),
            Map.entry("14.1", OperationType.SEGMENT_OP),
            Map.entry("14.2", OperationType.SEGMENT_OP),
            // Layer 17/22: Tablespace / LMT
            Map.entry("17.1", OperationType.TABLESPACE_OP),
            Map.entry("17.2", OperationType.TABLESPACE_OP),
            Map.entry("22.1", OperationType.TABLESPACE_OP),
            // Layer 24: DDL
            Map.entry("24.1", OperationType.DDL),
            Map.entry("24.2", OperationType.DDL),
            Map.entry("24.3", OperationType.DDL)
    );
    private final int recordLimit;

    public RedoParser(int recordLimit, Deserializer deserializer) {
        this.recordLimit = recordLimit;
        this.deserializer = deserializer;
    }

    private byte[] lastRecord ;

    private int lastRecordLen = 0;
    private int copiedRecordLen = 0;
    private int needCopyLen = 0;
    private Deserializer deserializer;


    public RedoParseResult parse(Path file) throws IOException {
        Objects.requireNonNull(file, "file");
        List<RedoRecord> records = new ArrayList<>();
        List<RedoChange> dml = new ArrayList<>();
        List<RedoChange> ddl = new ArrayList<>();

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            int blockSize = detectBlockSize(channel);
            int seq = 0;

            long blockIndex = 0;
            for (long pos = 0; pos + blockSize <= fileSize; pos += blockSize, blockIndex++) {
                byte[] block = readBlock(channel, pos, blockSize);
                BlockHeader header = BlockHeader.parseBlockHeader(block);

                if (blockIndex < 2) { // block0 和 block1 只读取头信息，不解析记录
                    seq = Math.toIntExact(header.sequence());
                    continue;
                }else {
                    if (header.sequence()!=seq){
                        System.out.println("finished block index:" + blockIndex);
                        break;
                    }
                }

                if (lastRecord != null) {
                    if (needCopyLen > blockSize - BlockHeader.BLOCK_HEADER_SIZE) {
                        System.arraycopy(block, BlockHeader.BLOCK_HEADER_SIZE,lastRecord ,
                                copiedRecordLen, blockSize - BlockHeader.BLOCK_HEADER_SIZE);
                        copiedRecordLen+=(blockSize - BlockHeader.BLOCK_HEADER_SIZE);
                        needCopyLen = lastRecord.length - copiedRecordLen;
                        continue;
                    }else {
                        System.arraycopy(block, BlockHeader.BLOCK_HEADER_SIZE,lastRecord, copiedRecordLen, needCopyLen);
                        RedoRecord redoRecord = RedoRecordParser.parseRedoRecord(header, lastRecord);
                        ConvertRedoRecord convert = RecordConvertor.convert(redoRecord, lastRecord);
                        deserializer.processRecord(convert);
                        if (needCopyLen > blockSize - 24 - 16) {
                            lastRecord = null;
                            copiedRecordLen = 0;
                            needCopyLen = 0;
                            // 没法满足record
                            continue;
                        }
                        lastRecord = null;
                        copiedRecordLen = 0;
                        needCopyLen = 0;

                    }
                }

                int offset = header.offset() & 0x7FFF; // 最高位舍弃
                if (offset <= 0 || offset >= blockSize) {
                    continue;
                }

                int cursor = offset;
                while (cursor + 4 <= blockSize) {
//                    long recordPos = pos + cursor;
                    int recordLen = BinaryUtil.getU32(block, cursor);
//                    System.out.println("current block: " + blockIndex + ", record len: " + recordLen);
                    if (recordLen <= 0) { // 简单防护
                        break;
                    }
                    // 计算当前偏移量是否大于当前len
                    if (cursor + recordLen > blockSize) {
                        lastRecord = new byte[recordLen];
                        System.arraycopy(block, cursor, lastRecord, 0, blockSize - cursor);
                        copiedRecordLen = blockSize - cursor;
                        needCopyLen = recordLen - copiedRecordLen;
                        break;
                    }else {


                        byte[] record = new byte[recordLen];
                        System.arraycopy(block, cursor, record, 0, recordLen);
                        RedoRecord redoRecord = RedoRecordParser.parseRedoRecord(header,record);
                        ConvertRedoRecord convert = RecordConvertor.convert(redoRecord, record);
                        deserializer.processRecord(convert);
//                        System.out.println("current block: " + blockIndex + ", record len: " + recordLen);
                        if (cursor + recordLen > blockSize - 24) {
                            // 没法满足record
                            break;
                        }
                        cursor += recordLen;
                    }
                }
            }
            return new RedoParseResult(blockSize, records, dml, ddl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int readIntLE(ByteBuffer buffer, long pos) {
        buffer.position((int) pos);
        return buffer.getInt();
    }

    private int detectBlockSize(FileChannel channel) throws IOException {
        ByteBuffer fh0 = BinaryUtil.allocateLE(512);
        BinaryUtil.readFully(channel, 0, fh0);
        int candidate = fh0.getInt(FH0_BLOCKSIZE_OFFSET);
        if (candidate <= 0 || candidate > 8192) {
            return DEFAULT_BLOCKSIZE;
        }
        return candidate;
    }

    private byte[] readBlock(FileChannel channel, long blockStart, int blockSize) throws IOException {
        byte[] block = new byte[blockSize];
        BinaryUtil.readFully(channel, blockStart, ByteBuffer.wrap(block));
        return block;
    }

    private ChangeCategory categorize(OperationType op) {
        return switch (op) {
            case INSERT_ROW, UPDATE_ROW, DELETE_ROW, BEGIN_TX, COMMIT_TX, ROLLBACK_TX, UNDO -> ChangeCategory.DML;
            case DDL, INDEX_OP, SEGMENT_OP, TABLESPACE_OP -> ChangeCategory.DDL;
            default -> ChangeCategory.UNKNOWN;
        };
    }

    private int readIntLE(FileChannel channel, long position) throws IOException {
        ByteBuffer buf = BinaryUtil.allocateLE(4);
        BinaryUtil.readFully(channel, position, buf);
        return buf.getInt();
    }

    private int align4(int len) {
        int rem = len % 4;
        return rem == 0 ? len : len + (4 - rem);
    }


    private OperationType mapOperation(int layer, int code, int typ) {
        OperationType mapped = OPCODE_MAP.get(key(layer, code));
        if (mapped != null) {
            return mapped;
        }
        // typ 5/6 也可能出现在 DDL 分支
        if (typ == 5 || typ == 6) {
            return OperationType.DDL;
        }
        // fallback：层级推断
        if (layer == 4) {
            return OperationType.BLOCK_CLEANOUT;
        }
        if (layer == 5) {
            return OperationType.UNDO;
        }
        if (layer == 10) {
            return OperationType.INDEX_OP;
        }
        if (layer == 11) {
            return OperationType.UNKNOWN;
        }
        if (layer == 13 || layer == 14) {
            return OperationType.SEGMENT_OP;
        }
        if (layer == 17 || layer == 22) {
            return OperationType.TABLESPACE_OP;
        }
        if (layer == 24) {
            return OperationType.DDL;
        }
        return OperationType.UNKNOWN;
    }

    private String key(int layer, int code) {
        return layer + "." + code;
    }

    private RowData decodeRow(List<Integer> segmentLengths, List<String> segmentHex) {
        if (segmentLengths.isEmpty() || segmentHex.isEmpty()) {
            return null;
        }
        String hex = segmentHex.get(0);
        byte[] bytes = hexStringToBytes(hex);
        if (bytes.length < 3) {
            return null;
        }
        int flag = Byte.toUnsignedInt(bytes[0]);
        int lock = Byte.toUnsignedInt(bytes[1]);
        int columnCount = Byte.toUnsignedInt(bytes[2]);
        List<ColumnData> columns = new ArrayList<>();
        int idx = 3;
        for (int i = 0; i < columnCount && idx < bytes.length; i++) {
            int len = Byte.toUnsignedInt(bytes[idx++]);
            if (len == 0xFF) { // 0xFF 通常表示 NULL
                columns.add(new ColumnData(len, true, "", ""));
                continue;
            }
            if (idx + len > bytes.length) {
                break;
            }
            byte[] col = new byte[len];
            System.arraycopy(bytes, idx, col, 0, len);
            idx += len;
            columns.add(new ColumnData(
                    len,
                    false,
                    bytesToHex(col),
                    toAscii(col)
            ));
        }
        return new RowData(flag, lock, columnCount, columns);
    }

    private String bytesToHex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    private String toAscii(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            int v = b & 0xFF;
            if (v >= 32 && v <= 126) {
                sb.append((char) v);
            } else {
                sb.append('.');
            }
        }
        return sb.toString();
    }

    private byte[] hexStringToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}

