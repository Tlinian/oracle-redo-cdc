package com.example.redo.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
public class Xid {
    private int xid0;
    private int xid1;
    private int xid2;
    private int xid3;

    @Override
    public String toString() {
        // 转成16进制字符串
        return String.format("0x%04x.%04x.%04x.%04x", xid0, xid1, xid2, xid3);
    }
}
