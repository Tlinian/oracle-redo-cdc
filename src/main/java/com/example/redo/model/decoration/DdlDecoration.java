package com.example.redo.model.decoration;

import com.example.redo.ConvertRedoRecord;
import com.example.redo.deserialize.RBA;
import com.example.redo.model.ChangeCode;
import com.example.redo.model.Xid;
import com.example.redo.model.origin.RedoChange;
import com.example.redo.model.origin.RedoRecord;
import com.example.redo.util.BinaryUtil;
import lombok.Builder;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

@Getter
@Builder
public class DdlDecoration implements  RecordDecoration {
    private long scn;
    private RBA rba;
    private long conUid;
    private Xid xid;
    int[] beforeCols;
    int objId;
    String sql;

    @Override
    public ChangeCode getChangeCode() {
        return ChangeCode.DELETE;
    }

    public static DdlDecoration parse(RedoRecord record, byte[] recordBytes) {
        RedoChange ddlChange = null;
        for (RedoChange change : record.changes()) {
            if (change.changeCode().equals(ChangeCode.DDL)){
                ddlChange = change;
            }
        }
        if (ddlChange == null){
            return null;
        }

        int[][] vectors = ddlChange.vectors();
        int objId = BinaryUtil.getU32(recordBytes,vectors[0x0B][1]);
        String ddlSql = new String(Arrays.copyOfRange(recordBytes, vectors[7][1], vectors[7][1] + vectors[7][0]-1));
        return DdlDecoration.builder().scn(record.scn())
                .rba(new RBA(record.sequence(), record.offset(), record.blockNumber()))
                .conUid(record.conUid())
                .objId(objId)
                .sql(ddlSql)
                .build();
    }
}
