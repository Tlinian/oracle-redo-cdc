package com.example.redo.model;

import java.util.List;

public record RedoChange(
        ChangeHeader changeHeader,
        int data_object_id,
        int[][] vectors,
        int changeLength,
        ChangeCode changeCode
) {
}

