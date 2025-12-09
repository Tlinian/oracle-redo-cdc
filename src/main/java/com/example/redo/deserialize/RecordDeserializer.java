package com.example.redo.deserialize;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.config.Config;
import com.example.redo.metadata.ColumnMeta;
import com.example.redo.metadata.MetadataManager;
import com.example.redo.metadata.TableId;
import com.example.redo.metadata.TableMetadata;
import com.example.redo.model.ChangeCode;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class RecordDeserializer implements Deserializer {
    private Config config;
    MetadataManager metadataManager;
    ArrayBlockingQueue<RedoEvent> redoEvents;

    public RecordDeserializer(Config config, MetadataManager metadataManager, ArrayBlockingQueue<RedoEvent> redoEvents) {
        this.config = config;
        this.metadataManager = metadataManager;
        this.redoEvents = redoEvents;
    }

    public void processRecord(ConvertRedoRecord record) {
        if (record == null) {
            return;
        }
        if (!metadataManager.getChecker().check(record.getConUid(), record.getObjId())) {
            return;
        }
        RedoEvent e = deserializeDmlRecord(record);
        if (e != null) {
            try {
                redoEvents.put(e);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public RedoEvent deserializeDmlRecord(ConvertRedoRecord redoRecord) {
        switch (redoRecord.getChangeCode()) {
            case DDL:
                return new DdlEvent(redoRecord.getScn(), redoRecord.getScn(), EventType.DDL, redoRecord.getDdlSql());
            case COMMIT:
                return new CommitEvent(redoRecord.getScn(), redoRecord.getScn());
            case INSERT:
                return insertRecord(redoRecord);
            case DELETE:
                return deleteRecord(redoRecord);
            case UPDATE:
                return updateRecord(redoRecord);
            default:
                return null;
        }
    }

    public RedoEvent insertRecord(ConvertRedoRecord record) {
        List<byte[]> after = record.getAfter();
        int[] afterCols = record.getAfterCols();
        TableId tableId = metadataManager.getTableIdMap().get(record.getObjId());
        TableMetadata tableMetadata = metadataManager.getTableMetadataMap().get(tableId);
        List<Object> afterData = new ArrayList<>();
        for (int i = 0; i < after.size(); i++) {
            afterData.add(tableMetadata.getColumnIdMap().get(afterCols[i]).convertData(after.get(i)));
        }
        return new DmlEvent(record.getScn(), record.getBlk(), record.getOffset(), record.getSeq(), record.getXid(),
                record.getScn(), EventType.INSERT, tableId, record.getObjId(), null, afterData);
    }

    public RedoEvent deleteRecord(ConvertRedoRecord record) {
        List<byte[]> before = record.getBefore();
        int[] beforeCols = record.getBeforeCols();
        TableId tableId = metadataManager.getTableIdMap().get(record.getObjId());
        TableMetadata tableMetadata = metadataManager.getTableMetadataMap().get(tableId);
        List<Object> afterData = new ArrayList<>();
        for (int i = 0; i < before.size(); i++) {
            afterData.add(tableMetadata.getColumnIdMap().get(beforeCols[i]).convertData(before.get(i)));
        }
        return new DmlEvent(record.getScn(), record.getBlk(), record.getOffset(), record.getSeq(), record.getXid(),
                record.getScn(), EventType.DELETE, tableId, record.getObjId(), afterData, null);
    }

    public RedoEvent updateRecord(ConvertRedoRecord record) {
        List<byte[]> before = record.getBefore();
        int[] beforeCols = record.getBeforeCols();
        TableId tableId = metadataManager.getTableIdMap().get(record.getObjId());
        TableMetadata tableMetadata = metadataManager.getTableMetadataMap().get(tableId);
        Map<Integer, ColumnMeta> columnIdMap = tableMetadata.getColumnIdMap();
        Map<Integer, Object> beforeData = new HashMap<>();
        for (int i = 0; i < before.size(); i++) {

            beforeData.put(beforeCols[i], columnIdMap.get(beforeCols[i]).convertData(before.get(i)));
        }

        List<byte[]> other = record.getOther();
        int[] otherCols = record.getOtherCols();
        for (int i = 0; i < other.size(); i++) {
            beforeData.put(otherCols[i], columnIdMap.get(otherCols[i]).convertData(other.get(i)));
        }

        List<byte[]> after = record.getAfter();
        int[] afterCols = record.getAfterCols();
        List<Object> afterData = new ArrayList<>();
        for (int i = 0; i < after.size(); i++) {
            afterData.add(columnIdMap.get(afterCols[i]).convertData(after.get(i)));
        }
        return new DmlEvent(record.getScn(), record.getBlk(), record.getOffset(), record.getSeq(), record.getXid(),
                record.getScn(), EventType.UPDATE, tableId, record.getObjId(), Arrays.asList(beforeData.values().toArray()), afterData);
    }
}
