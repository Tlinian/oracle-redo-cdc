package com.xdream.redo.model.decoration;

import com.xdream.redo.deserialize.RBA;
import com.xdream.redo.model.ChangeCode;
import com.xdream.redo.model.Xid;

public interface RecordDecoration {

    RBA getRba();

    long getScn();

    ChangeCode getChangeCode();

    long getConUid();

    Xid getXid();
}
