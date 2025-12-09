package com.example.redo.deserialize;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class RBA {
    private long seq;
    private int offset;
    private long blk;
}
