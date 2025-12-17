package com.xdream.redo.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TableId {
    private String tableName;
    private String schema;

    @Override
    public String toString() {
        return schema + "." + tableName;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TableId tableId = (TableId) obj;
        return tableName.equals(tableId.tableName) && schema.equals(tableId.schema);
    }
}
