package com.example.redo.model;

import com.example.redo.ConvertLlbRedoRecord;
import com.example.redo.ConvertRedoRecord;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class OracleTranction {
    private Xid xid;
    private List<ConvertRedoRecord> redoChanges = new ArrayList<>();

    public List<ConvertRedoRecord> convertRedoChanges() {
        List<ConvertRedoRecord> convertRedoChanges = new ArrayList<>();
        List<ConvertRedoRecord> llbChanges = new ArrayList<>();
        for (ConvertRedoRecord record : redoChanges) {
            if (record.getChangeCode() == ChangeCode.LLB){
                llbChanges.add(record);
            }else if (record.getChangeCode() == ChangeCode.INSERT || record.getChangeCode() == ChangeCode.UPDATE){
                for (ConvertRedoRecord record2 : llbChanges) {
                    if (record2 instanceof ConvertLlbRedoRecord llbRedoRecord) {
                        int columnId = llbRedoRecord.getColumnId();
                        int lSize = llbRedoRecord.getLSize();
                        byte[] bytes = record.getAfter().get(columnId - 1);
                        record.getAfter().set(columnId-1, Arrays.copyOfRange(bytes, bytes.length-lSize,bytes.length));
                    }
                }
                convertRedoChanges.add(record);
            }else {
                convertRedoChanges.add(record);
            }
        }
        return convertRedoChanges;
    }
}
