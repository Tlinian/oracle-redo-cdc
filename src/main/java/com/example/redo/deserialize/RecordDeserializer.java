package com.example.redo.deserialize;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.config.Config;
import com.example.redo.metadata.ColumnMeta;
import com.example.redo.metadata.MetadataManager;
import com.example.redo.metadata.TableId;
import com.example.redo.metadata.TableMetadata;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.decoration.*;

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

    public void processRecord(RecordDecoration record) {
        if (record == null) {
            return;
        }
        if (record instanceof DeleteDecoration decoration){
            if (!metadataManager.getChecker().check(record.getConUid(), decoration.getObjId())) {
                return;
            }
        }else if (record instanceof InsertDecoration decoration){
            if (!metadataManager.getChecker().check(record.getConUid(), decoration.getObjId())) {
                return;
            }
        }else if (record instanceof UpdateDecoration decoration){
            if (!metadataManager.getChecker().check(record.getConUid(), decoration.getObjId())) {
                return;
            }
        }
        List<RedoEvent> e = deserializeDmlRecord(record);
        if (e != null) {
            redoEvents.addAll(e);
        }
    }

    public List<RedoEvent> deserializeDmlRecord(RecordDecoration redoRecord) {
        switch (redoRecord.getChangeCode()) {
            case DDL:
                DdlEvent ddlEvent = DdlEvent.builder().scn(redoRecord.getScn()).commitScn(redoRecord.getScn())
                        .kind(((DdlDecoration) redoRecord).getKind()).eventType(EventType.DDL).sql(((DdlDecoration) redoRecord).getSql()).xid(redoRecord.getXid()).build();
                return Collections.singletonList(ddlEvent);
            case COMMIT:
                return Collections.singletonList(CommitEvent.builder().commitScn(redoRecord.getScn()).scn(redoRecord.getScn()).xid(redoRecord.getXid()).build());
            case INSERT:
                return insertRecord((InsertDecoration) redoRecord);
            case INSERT_MULTI:
                return insertMultiRecord((InsertMultiDecoration) redoRecord);
            case DELETE:
                return deleteRecord((DeleteDecoration) redoRecord);
            case UPDATE:
                return updateRecord((UpdateDecoration) redoRecord);
            default:
                return null;
        }
    }

    public List<RedoEvent> insertMultiRecord(InsertMultiDecoration record) {
        List<List<byte[]>> datas = record.getDatas();
        int[] afterCols = record.getAfterCols();
        TableId tableId = metadataManager.getTableIdMap().get(record.getObjId());
        TableMetadata tableMetadata = metadataManager.getTableMetadataMap().get(tableId);
        List<RedoEvent> insertEvents = new ArrayList<>();
        for (int i = 0; i < datas.size(); i++) {
            List<Object> afterData = new ArrayList<>();
            List<byte[]> after = datas.get(i);
            for (int j = 0; j < after.size(); j++) {
                afterData.add(tableMetadata.getColumnIdMap().get(afterCols[j]).convertData(after.get(j)));
            }
            InsertEvent insertEvent = InsertEvent.builder().scn(record.getScn())
                    .commitScn(record.getScn()).tableId(tableId).objId(record.getObjId()).afterCols(afterCols).after(afterData).xid(record.getXid()).build();
            insertEvents.add(insertEvent);
        }
        return insertEvents;
    }

    public List<RedoEvent> insertRecord(InsertDecoration record) {
        List<byte[]> after = record.getAfter();
        int[] afterCols = record.getAfterCols();
        TableId tableId = metadataManager.getTableIdMap().get(record.getObjId());
        TableMetadata tableMetadata = metadataManager.getTableMetadataMap().get(tableId);
        List<Object> afterData = new ArrayList<>();
        for (int i = 0; i < after.size(); i++) {
            afterData.add(tableMetadata.getColumnIdMap().get(afterCols[i]).convertData(after.get(i)));
        }
        InsertEvent insertEvent = InsertEvent.builder().scn(record.getScn())
                .commitScn(record.getScn()).tableId(tableId).objId(record.getObjId()).afterCols(afterCols).after(afterData).xid(record.getXid()).build();
        return Collections.singletonList(insertEvent);
    }

    public List<RedoEvent> deleteRecord(DeleteDecoration record) {
        List<byte[]> before = record.getBefore();
        int[] beforeCols = record.getBeforeCols();
        TableId tableId = metadataManager.getTableIdMap().get(record.getObjId());
        TableMetadata tableMetadata = metadataManager.getTableMetadataMap().get(tableId);
        List<Object> afterData = new ArrayList<>();
        for (int i = 0; i < before.size(); i++) {
            afterData.add(tableMetadata.getColumnIdMap().get(beforeCols[i]).convertData(before.get(i)));
        }
        DeleteEvent deleteEvent = DeleteEvent.builder().scn(record.getScn())
                .commitScn(record.getScn()).tableId(tableId).objId(record.getObjId()).beforeCols(beforeCols).before(afterData).xid(record.getXid()).build();
        return Collections.singletonList(deleteEvent);
    }

    public List<RedoEvent> updateRecord(UpdateDecoration record) {
        List<byte[]> before = record.getBefore();
        int[] beforeCols = record.getBeforeCols();
        List<Integer> beforeColsList = new ArrayList<>();
        for (int i = 0; i < beforeCols.length; i++) {
            beforeColsList.add(beforeCols[i]);
        }
        TableId tableId = metadataManager.getTableIdMap().get(record.getObjId());
        TableMetadata tableMetadata = metadataManager.getTableMetadataMap().get(tableId);
        Map<Integer, ColumnMeta> columnIdMap = tableMetadata.getColumnIdMap();
        List<Object> beforeData = new ArrayList<>();
        for (int i = 0; i < before.size(); i++) {
            beforeData.add(columnIdMap.get(beforeCols[i]).convertData(before.get(i)));
        }


        List<byte[]> after = record.getAfter();
        int[] afterCols = record.getAfterCols();
        List<Integer> afterColsList = new ArrayList<>();
        for (int i = 0; i < afterCols.length; i++) {
            afterColsList.add(afterCols[i]);
        }
        List<Object> afterData = new ArrayList<>();
        for (int i = 0; i < after.size(); i++) {
            afterData.add(columnIdMap.get(afterCols[i]).convertData(after.get(i)));
        }

        List<byte[]> other = record.getOther();
        int[] otherCols = record.getOtherCols();
        for (int i = 0; i < other.size(); i++) {
            int otherCol = otherCols[i];
            for (int j = 0; j < afterColsList.size(); j++) {
                if (otherCol < afterColsList.get(j)) {
                    afterColsList.add(j, otherCol);
                    afterData.add(j,columnIdMap.get(otherCol).convertData(other.get(i)));
                    break;
                }else if (otherCol == afterColsList.get(j)) {
                    break;
                }
            }

            for (int j = 0; j < beforeColsList.size(); j++) {
                if (otherCol < beforeColsList.get(j)) {
                    beforeColsList.add(j, otherCol);
                    beforeData.add(j,columnIdMap.get(otherCol).convertData(other.get(i)));
                    break;
                }else if (otherCol == beforeColsList.get(j)) {
                    break;
                }
            }
        }

        UpdateEvent updateEvent = UpdateEvent.builder().scn(record.getScn())
                .commitScn(record.getScn()).tableId(tableId).objId(record.getObjId())
                .beforeCols(beforeColsList.stream().mapToInt(i -> i).toArray())
                .afterCols(afterColsList.stream().mapToInt(i -> i).toArray()).before(beforeData).after(afterData).xid(record.getXid()).build();
        return Collections.singletonList(updateEvent);
    }
}
