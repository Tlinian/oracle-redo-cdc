package com.example.redo.deserialize;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.model.decoration.RecordDecoration;

public class PrintDeserializer implements Deserializer {
    @Override
    public void processRecord(RecordDecoration record) {
        System.out.println(record);
    }
}
