package com.example.redo.model.decoration;

import com.example.redo.deserialize.RBA;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.Xid;

public interface RecordDecoration {

    RBA getRba();

    long getScn();

    ChangeCode getChangeCode();

    long getConUid();

    Xid getXid();
}
