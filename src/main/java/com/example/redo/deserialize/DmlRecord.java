package com.example.redo.deserialize;

import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
public class DmlRecord {
    private long scn;
    private int blk;
    private int offset;
    private int seq;
    private String xid;
    int[] beforeCols;
    int[] afterCols;
    int objId;
    List<Object> after = new ArrayList<>();
    List<Object> before = new ArrayList<>();

}
