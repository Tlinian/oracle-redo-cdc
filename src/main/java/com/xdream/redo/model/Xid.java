package com.xdream.redo.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Xid xid = (Xid) o;
        return xid0 == xid.xid0 && xid1 == xid.xid1 && xid2 == xid.xid2 && xid3 == xid.xid3;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xid0, xid1, xid2, xid3);
    }
}
