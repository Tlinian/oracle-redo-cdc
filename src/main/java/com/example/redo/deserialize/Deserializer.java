package com.example.redo.deserialize;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.model.decoration.RecordDecoration;

public interface Deserializer {
    void processRecord(RecordDecoration record);
}
