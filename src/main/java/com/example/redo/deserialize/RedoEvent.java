package com.example.redo.deserialize;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public abstract class RedoEvent {
        private long scn;
        private long commitScn;
        private EventType type;

}
