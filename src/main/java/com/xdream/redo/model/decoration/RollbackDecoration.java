package com.xdream.redo.model.decoration;

import com.xdream.redo.deserialize.RBA;
import com.xdream.redo.model.ChangeCode;
import com.xdream.redo.model.Xid;
import com.xdream.redo.model.origin.RedoChange;
import com.xdream.redo.model.origin.RedoRecord;
import com.xdream.redo.util.BinaryUtil;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RollbackDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] beforeCols;
    int objId;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.ROLLBACK;
    }
}
