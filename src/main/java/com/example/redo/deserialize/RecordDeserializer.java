package com.example.redo.deserialize;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.RedoRecord;

public class RecordDeserializer {


    public void processRecord(ConvertRedoRecord record) {
//        DmlRecord dmlRecord = deserializeDmlRecord(record);
//        if (dmlRecord)
//        System.out.println(dmlRecord);
        if (record == null) {
            return;
        }
        if (record.getObjId() == 73474) {
//            DmlRecord dmlRecord = deserializeDmlRecord(record);
            System.out.println(record);
        }else if (record.getChangeCode() == ChangeCode.DDL || record.getChangeCode() == ChangeCode.COMMIT) {
            System.out.println(record);
        }
    }

    public DmlRecord deserializeDmlRecord(ConvertRedoRecord bytes) {

        return null;
    }
}
