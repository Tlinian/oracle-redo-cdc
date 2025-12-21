package com.xdream.redo.parser;

import com.xdream.redo.metadata.Checker;
import com.xdream.redo.model.ChangeCode;
import com.xdream.redo.model.decoration.*;
import com.xdream.redo.model.origin.RedoChange;
import com.xdream.redo.model.origin.RedoRecord;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.List;

@Slf4j
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
           case DELETE_MULTI -> {
               return DeleteMultiDecoration.parse(record,recordBytes);
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
