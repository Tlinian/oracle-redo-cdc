package com.xdream.redo.deserialize;

import com.xdream.redo.model.decoration.RecordDecoration;

public interface Deserializer {
    void processRecord(RecordDecoration record);
}
