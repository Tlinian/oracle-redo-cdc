package com.example.redo.model.origin;

import com.example.redo.model.ChangeCode;

public record RedoChange(
        ChangeHeader changeHeader,
        int data_object_id,
        int[][] vectors,
        int changeLength,
        ChangeCode changeCode
) {
}

