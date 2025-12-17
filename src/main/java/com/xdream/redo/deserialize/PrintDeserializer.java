package com.xdream.redo.deserialize;

import com.xdream.redo.model.decoration.RecordDecoration;

public class PrintDeserializer implements Deserializer {
    @Override
    public void processRecord(RecordDecoration record) {
        System.out.println(record);
    }
}
