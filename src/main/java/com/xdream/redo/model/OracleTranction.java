package com.xdream.redo.model;

import com.example.redo.model.decoration.*;
import com.xdream.redo.model.decoration.*;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public class OracleTranction {
    private Xid xid;
    private List<RecordDecoration> redoChanges = new ArrayList<>();



    public List<RecordDecoration> convertRedoChanges() {
        List<RecordDecoration> convertRedoChanges = new ArrayList<>();
        List<RecordDecoration> llbChanges = new ArrayList<>();
        List<RecordDecoration> lobChanges = new ArrayList<>();
        for (RecordDecoration record : redoChanges) {
            if (record.getChangeCode() == ChangeCode.LLB){
                llbChanges.add(record);
            } else if (record.getChangeCode() == ChangeCode.LOB_KDLIRBIMG){
                lobChanges.add(record);
            } else if (record instanceof InsertDecoration insertRecord){
                merge(insertRecord, llbChanges, convertRedoChanges, lobChanges);
            }else if (record instanceof UpdateDecoration updateRecord){
                merge(updateRecord, llbChanges, convertRedoChanges, lobChanges);
            }else {
                convertRedoChanges.add(record);
            }
        }
        return convertRedoChanges;
    }

    private static void merge(InsertDecoration record, List<RecordDecoration> llbChanges, List<RecordDecoration> convertRedoChanges, List<RecordDecoration> lobChanges) {
        for (RecordDecoration record2 : llbChanges) {
            if (record2 instanceof LlbDecoration llbRedoRecord) {
                int columnId = llbRedoRecord.getColumnId();
                int lSize = llbRedoRecord.getLSize();
                byte[] bytes = record.getAfter().get(columnId - 1);
                if (bytes.length >= lSize){
                    record.getAfter().set(columnId-1, Arrays.copyOfRange(bytes, bytes.length-lSize,bytes.length));
                }
            }
        }
        Set<Integer> columnIdSet = new HashSet<>();
        for (RecordDecoration record2 : lobChanges) {
            if (record2 instanceof LobKdlirbimgDecoration lobRedoRecord) {
                int columnId = lobRedoRecord.getColId();
                int lSize = lobRedoRecord.getLobData().length;
                if (!columnIdSet.contains(columnId)){
                    record.getAfter().set(columnId-1, lobRedoRecord.getLobData());
                    columnIdSet.add(columnId);
                }else {
                    byte[] bytes = record.getAfter().get(columnId - 1);
                    byte[] lobData = lobRedoRecord.getLobData();
                    byte[] newBytes = new byte[bytes.length + lobData.length];
                    System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                    System.arraycopy(lobData, 0, newBytes, bytes.length, lobData.length);
                    record.getAfter().set(columnId-1, newBytes);
                }
            }
        }
        convertRedoChanges.add(record);
        llbChanges.clear();
    }

    private static void merge(UpdateDecoration record, List<RecordDecoration> llbChanges, List<RecordDecoration> convertRedoChanges, List<RecordDecoration> lobChanges) {
        for (RecordDecoration record2 : llbChanges) {
            if (record2 instanceof LlbDecoration llbRedoRecord) {
                int columnId = llbRedoRecord.getColumnId();
                int lSize = llbRedoRecord.getLSize();
                int colIndex = columnId - 1;
                for (int i = 0; i < record.getAfterCols().length; i++) {
                    if (record.getAfterCols()[i] == columnId){
                        colIndex = i;
                    }
                }
                byte[] bytes = record.getAfter().get(colIndex);
                record.getAfter().set(colIndex, Arrays.copyOfRange(bytes, bytes.length-lSize,bytes.length));
            }
        }
        Set<Integer> columnIdSet = new HashSet<>();
        for (RecordDecoration record2 : lobChanges) {
            if (record2 instanceof LobKdlirbimgDecoration lobRedoRecord) {
                int columnId = lobRedoRecord.getColId();
                int lSize = lobRedoRecord.getLobData().length;
                if (!columnIdSet.contains(columnId)){
                    record.getAfter().set(columnId-1, lobRedoRecord.getLobData());
                    columnIdSet.add(columnId);
                }else {

                    byte[] bytes = record.getAfter().get(columnId - 1);
                    byte[] lobData = lobRedoRecord.getLobData();
                    byte[] newBytes = new byte[bytes.length + lobData.length];
                    System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                    System.arraycopy(lobData, 0, newBytes, bytes.length, lobData.length);
                    record.getAfter().set(columnId-1, newBytes);
                }
            }
        }
        convertRedoChanges.add(record);
        llbChanges.clear();
    }
}
