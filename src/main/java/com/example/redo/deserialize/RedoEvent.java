package com.example.redo.deserialize;


import com.example.redo.model.Xid;

public interface RedoEvent {

        long getScn();
        long getCommitScn();
        EventType getEventType();

        Xid getXid();
}
