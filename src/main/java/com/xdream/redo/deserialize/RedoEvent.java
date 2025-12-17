package com.xdream.redo.deserialize;


import com.xdream.redo.model.Xid;

public interface RedoEvent {

        long getScn();
        long getCommitScn();
        EventType getEventType();

        Xid getXid();
}
