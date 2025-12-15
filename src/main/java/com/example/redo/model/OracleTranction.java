package com.example.redo.model;

import com.example.redo.ConvertLlbRedoRecord;
import com.example.redo.ConvertRedoRecord;
import com.example.redo.model.decoration.InsertDecoration;
import com.example.redo.model.decoration.LlbDecoration;
import com.example.redo.model.decoration.RecordDecoration;
import com.example.redo.model.decoration.UpdateDecoration;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class OracleTranction {
    private Xid xid;
    private List<RecordDecoration> redoChanges = new ArrayList<>();



    public List<RecordDecoration> convertRedoChanges() {
        List<RecordDecoration> convertRedoChanges = new ArrayList<>();
        List<RecordDecoration> llbChanges = new ArrayList<>();
        for (RecordDecoration record : redoChanges) {
            if (record.getChangeCode() == ChangeCode.LLB){
                llbChanges.add(record);
            }else if (record instanceof InsertDecoration insertRecord){
                merge(insertRecord, llbChanges, convertRedoChanges);
            }else if (record instanceof UpdateDecoration updateRecord){
                merge(updateRecord, llbChanges, convertRedoChanges);
            }else {
                convertRedoChanges.add(record);
            }
        }
        return convertRedoChanges;
    }

    private static void merge(InsertDecoration record, List<RecordDecoration> llbChanges, List<RecordDecoration> convertRedoChanges) {
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
        convertRedoChanges.add(record);
        llbChanges.clear();
    }

    private static void merge(UpdateDecoration record, List<RecordDecoration> llbChanges, List<RecordDecoration> convertRedoChanges) {
        for (RecordDecoration record2 : llbChanges) {
            if (record2 instanceof LlbDecoration llbRedoRecord) {
                int columnId = llbRedoRecord.getColumnId();
                int lSize = llbRedoRecord.getLSize();
                byte[] bytes = record.getAfter().get(columnId - 1);
                record.getAfter().set(columnId-1, Arrays.copyOfRange(bytes, bytes.length-lSize,bytes.length));
            }
        }
        convertRedoChanges.add(record);
        llbChanges.clear();
    }
}
