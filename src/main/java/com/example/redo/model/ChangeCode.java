package com.example.redo.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.example.redo.model.OperationType.UNKNOWN;

@AllArgsConstructor
@Getter
public enum ChangeCode {
    INSERT((short) 0x0B02),
    DELETE((short) 0x0B03),
    UPDATE((short) 0x0B05),
    UNDO_SEM((short) 0x0502),
    UNDO_BEFORE((short) 0x0501),
    COMMIT((short) 0x0504),
    UNKNOWN((short) 0);

    private short code;

    public static ChangeCode getChangeCode(short code) {
        return switch (code) {
            case 0x0B02 -> INSERT;
            case 0x0B03 -> DELETE;
            case 0x0B05 -> UPDATE;
            case 0x0502 -> UNDO_SEM;
            case 0x0501 -> UNDO_BEFORE;
            case 0x0504 -> COMMIT;
            default -> UNKNOWN;
        };
    }
}
