package com.xdream.redo.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ChangeCode {
    INSERT((short) 0x0B02),
    DELETE((short) 0x0B03),
    UPDATE((short) 0x0B05),
    INSERT_MULTI((short) 0x0B0B),
    DELETE_MULTI((short) 0x0B0C),
    LLB((short) 0x0B11),
    UNDO_SEM((short) 0x0502),
    UNDO_BEFORE((short) 0x0501),
    COMMIT((short) 0x0504),
    ROLLBACK((short) 0xBBBB),
    DDL((short) 0x1801),
    LOB_REDO((short) 0x1A02),
    LOB_KDLIRBIMG((short) 0x1A06),
    LOB_UINDO_REDO((short) 0x1A01),
    LOAD_LOB((short) 0x1A06),
    UNKNOWN((short) 0);

    private short code;

    public static ChangeCode getChangeCode(short code) {
        for (ChangeCode changeCode : ChangeCode.values()) {
            if (changeCode.code == code) {
                return changeCode;
            }
        }
        return UNKNOWN;
    }
}
