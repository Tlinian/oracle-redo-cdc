package com.example.redo.deserialize;

import com.example.redo.ConvertRedoRecord;

public interface Deserializer {
    void processRecord(ConvertRedoRecord record);
}
