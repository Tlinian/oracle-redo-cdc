package com.example.redo.metadata;

public class Checker {
    private long conUid;

    private CheckerFunction checkerFunction;

    public Checker(long conUid,CheckerFunction checkerFunction) {
        this.conUid = conUid;
        this.checkerFunction = checkerFunction;
    }

    @FunctionalInterface
    public interface CheckerFunction {
        boolean check(int objId);
    }

    public boolean check(long conUid,int objId){
        return  checkerFunction.check(objId);
    }
}
