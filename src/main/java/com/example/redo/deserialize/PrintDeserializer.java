package com.example.redo.deserialize;

import com.example.redo.ConvertRedoRecord;

public class PrintDeserializer implements Deserializer {
    @Override
    public void processRecord(ConvertRedoRecord record) {
        System.out.println(record);
    }
}
