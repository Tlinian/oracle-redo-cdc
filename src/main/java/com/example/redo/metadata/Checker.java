package com.example.redo.metadata;

public class Checker {
    private int conUid;

    private CheckerFunction checkerFunction;

    public Checker(int conUid,CheckerFunction checkerFunction) {
        this.conUid = conUid;
        this.checkerFunction = checkerFunction;
    }

    @FunctionalInterface
    public interface CheckerFunction {
        boolean check(int objId);
    }

    public boolean check(int conUid,int objId){
        return this.conUid == conUid && checkerFunction.check(objId);
    }
}
