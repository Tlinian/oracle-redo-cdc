package com.example.redo.parser;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.deserialize.Deserializer;
import com.example.redo.deserialize.RBA;
import com.example.redo.metadata.Checker;
import com.example.redo.model.*;
import com.example.redo.model.decoration.RecordDecoration;
import com.example.redo.model.origin.RedoChange;
import com.example.redo.model.origin.RedoRecord;
import com.example.redo.util.BinaryUtil;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.*;

public class RedoParser {

    private static final int DEFAULT_BLOCKSIZE = 512;
    private static final int FH0_BLOCKSIZE_OFFSET = 5 * 4; // unknown0[5] 之后
    private final int recordLimit;
    private final Checker checker;
    private Map<Xid,OracleTranction> xidOracleTranctionMap = new HashMap<>();
    private long startScn;

    public RedoParser(int recordLimit, Deserializer deserializer,Checker checker, long startScn) {
        this.recordLimit = recordLimit;
        this.deserializer = deserializer;
        this.checker = checker;
        this.startScn = startScn;
    }

    private byte[] lastRecord ;

    private int lastRecordLen = 0;
    private int copiedRecordLen = 0;
    private int needCopyLen = 0;
    private Deserializer deserializer;
    @Getter
    private RBA dba;

    public RedoParseResult parse(Path file, RBA dba) throws IOException {
        this.dba = dba;
        return parse(file);
    }

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

                if (blockIndex < 2||(dba!=null&&header.sequence() == dba.getSeq()&&blockIndex < dba.getBlk())) { // block0 和 block1 只读取头信息，不解析记录
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
                        dealEvent(header, lastRecord);
                        dba = new RBA(header.sequence(), header.offset(), header.blockNumber());
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
                        dealEvent(header, record);
                        dba = new RBA(header.sequence(), header.offset(), header.blockNumber());
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

    private void dealEvent(BlockHeader header, byte[] lastRecord) throws SQLException {
        RedoRecord redoRecord = RedoRecordParser.parseRedoRecord(header, lastRecord);
        RecordDecoration convert = RecordConvertor.convert(redoRecord, lastRecord, checker);
        if (convert != null) {
            if (convert.getChangeCode() == ChangeCode.DDL){
                deserializer.processRecord(convert);
            }else if (xidOracleTranctionMap.containsKey(convert.getXid())) {
                OracleTranction oracleTranction = xidOracleTranctionMap.get(convert.getXid());
                if (convert.getChangeCode() == ChangeCode.COMMIT) {
                    if (convert.getScn() > startScn) {
                        oracleTranction.getRedoChanges().add(convert);
                        oracleTranction.convertRedoChanges().forEach(redoChange -> {
                            deserializer.processRecord(redoChange);
                        });
                    }
                    xidOracleTranctionMap.remove(convert.getXid());
                } else {
                    oracleTranction.getRedoChanges().add(convert);
                }
            } else {
                if (convert.getChangeCode() != ChangeCode.COMMIT) {
                    OracleTranction oracleTranction = new OracleTranction();
                    oracleTranction.getRedoChanges().add(convert);
                    oracleTranction.setXid(convert.getXid());
                    xidOracleTranctionMap.put(convert.getXid(), oracleTranction);
                }
            }
        }
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
}

