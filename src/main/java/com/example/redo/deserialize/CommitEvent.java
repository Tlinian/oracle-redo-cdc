package com.example.redo.deserialize;

public class CommitEvent extends RedoEvent{
    public CommitEvent(long scn, long commitScn) {
        super(scn, commitScn, EventType.COMMIT);
    }
}
