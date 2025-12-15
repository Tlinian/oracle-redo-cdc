package com.example.redo.model.origin;

import com.example.redo.model.ChangeCode;
import lombok.Setter;

import java.util.Objects;

public final class RedoChange {
    private final ChangeHeader changeHeader;
    @Setter
    private int dataObjectId;
    private final int[][] vectors;
    private final int changeLength;
    private final ChangeCode changeCode;

    public RedoChange(
            ChangeHeader changeHeader,
            int dataObjectId,
            int[][] vectors,
            int changeLength,
            ChangeCode changeCode
    ) {
        this.changeHeader = changeHeader;
        this.dataObjectId = dataObjectId;
        this.vectors = vectors;
        this.changeLength = changeLength;
        this.changeCode = changeCode;
    }

    public ChangeHeader changeHeader() {
        return changeHeader;
    }

    public int data_object_id() {
        return dataObjectId;
    }

    public int[][] vectors() {
        return vectors;
    }

    public int changeLength() {
        return changeLength;
    }

    public ChangeCode changeCode() {
        return changeCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RedoChange) obj;
        return Objects.equals(this.changeHeader, that.changeHeader) &&
                this.dataObjectId == that.dataObjectId &&
                Objects.equals(this.vectors, that.vectors) &&
                this.changeLength == that.changeLength &&
                Objects.equals(this.changeCode, that.changeCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changeHeader, dataObjectId, vectors, changeLength, changeCode);
    }

    @Override
    public String toString() {
        return "RedoChange[" +
                "changeHeader=" + changeHeader + ", " +
                "data_object_id=" + dataObjectId + ", " +
                "vectors=" + vectors + ", " +
                "changeLength=" + changeLength + ", " +
                "changeCode=" + changeCode + ']';
    }

}

