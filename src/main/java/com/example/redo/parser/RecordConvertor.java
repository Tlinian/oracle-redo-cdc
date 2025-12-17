package com.example.redo.parser;

import com.example.redo.metadata.Checker;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.decoration.*;
import com.example.redo.model.origin.RedoChange;
import com.example.redo.model.origin.RedoRecord;

import java.sql.SQLException;
import java.util.List;

public class RecordConvertor {
    public static RecordDecoration convert(RedoRecord record, byte[] recordBytes, Checker checker) throws SQLException {
        List<RedoChange> changes = record.changes();
        RedoChange redoChange = record.change();
        if (redoChange == null){
            return null;
        }

        ChangeCode changeCode = redoChange.changeCode();
       if (changeCode !=ChangeCode.COMMIT&&changeCode !=ChangeCode.DDL){
           if (checker != null && !checker.check(record.conUid(),redoChange.getDataObjectId())){
               return null;
           }
       }
       switch (changeCode){
           case INSERT -> {
               return InsertDecoration.parse(record,recordBytes);
           }
           case DELETE -> {
               return DeleteDecoration.parse(record,recordBytes);
           }
           case UPDATE -> {
               return UpdateDecoration.parse(record,recordBytes);
           }
           case COMMIT -> {
               return CommitDecoration.parse(record,recordBytes);
           }
           case LLB -> {
               return LlbDecoration.parse(record,recordBytes);
           }
           case DDL ->  {
               return DdlDecoration.parse(record,recordBytes);
           }
           case INSERT_MULTI -> {
               return InsertMultiDecoration.parse(record,recordBytes);
           }
           case UPDATE_MULTI -> {
               return InsertMultiDecoration.parse(record,recordBytes);
           }
           case LOB_KDLIRBIMG -> {
               return LobKdlirbimgDecoration.parse(record,recordBytes);
           }
           default -> {
               throw new SQLException("Unknown change code: " + changeCode);
           }
       }
    }

}
