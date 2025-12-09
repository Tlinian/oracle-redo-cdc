package com.example.redo.metadata;

import com.example.redo.config.Config;
import lombok.Getter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class MetadataManager {
    // URL, USER, PASSWORD
    private static final String URL = "jdbc:oracle:thin:@localhost:1521:ORCLCDB";
    private static final String QUERY_CON_UID = "select CON_ID,CON_UID from V$CONTAINERS where NAME = ?";
    private static final String QUERY_TABLE_ID = "select O.OBJECT_ID, O.CON_ID, T.OWNER, T.TABLE_NAME, T.DEPENDENCIES, P.PDB_NAME,\n" +
            "       decode(O.OBJECT_TYPE, 'TABLE', 'Y', 'N') IS_TABLE,\n" +
            "       decode(O.OBJECT_TYPE, 'TABLE', O.OBJECT_ID,\n" +
            "              (select PT.OBJECT_ID\n" +
            "               from   CDB_OBJECTS PT\n" +
            "               where  PT.OWNER=O.OWNER\n" +
            "                 and  PT.OBJECT_NAME=O.OBJECT_NAME\n" +
            "                 and  PT.CON_ID=O.CON_ID\n" +
            "                 and  PT.OBJECT_TYPE='TABLE')) PARENT_OBJECT_ID\n" +
            "from   CDB_OBJECTS O, CDB_PDBS P, CDB_TABLES T\n" +
            "where  O.OBJECT_TYPE in ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')\n" +
            "  and  O.TEMPORARY='N'\n" +
            "  and  O.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER','LBACSYS','DVSYS','WMSYS','EXFSYS')\n" +
            "  and  O.CON_ID=P.CON_ID (+)\n" +
            "  and  O.OWNER=T.OWNER\n" +
            "  and  O.OBJECT_NAME=T.TABLE_NAME and O.CON_ID = %s and O.OWNER in (%s)";

    private static final String QUERY_COLUMN_ID = "select OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_ID from DBA_TAB_COLUMNS where OWNER in (%s)";

    private static final String USER = "system";
    private static final String PASSWORD = "oracle";
    private Config config;
    private Connection connection;
    private int conUid;
    private int conId;


    private Map<Integer,TableId> tableIdMap = new HashMap<>();

    private Map<TableId,TableMetadata> tableMetadataMap = new HashMap<>();
    private Checker checker;

    public MetadataManager(Config config) {
        this.config = config;
    }

    public void init() {
        try {
            connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        initConUid();
        tableIdMap = initTableIdMapFromSchema();
        tableIdMap.putAll(initTableIdMapFromSchema());
        tableMetadataMap = initMetadataMapFromSchema();
        tableIdMap.putAll(initTableIdMapFromSchema());

        checker = new Checker(conUid, objId -> tableIdMap.containsKey(objId));
    }

    private void initConUid(){
        try (var statement = connection.prepareStatement(QUERY_CON_UID)) {
            statement.setString(1, config.getDatabase());
            var resultSet = statement.executeQuery();
            if (resultSet.next()) {
                conId = resultSet.getInt(1);
                conUid = resultSet.getInt(2);
            }else {
                throw  new SQLException("CON_UID not found for database: " + config.getDatabase());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Integer,TableId> initTableIdMapFromSchema() {
        var schemaList = config.getSchemaList().split(",");
        String sql = String.format(QUERY_TABLE_ID, conId, Arrays.stream(schemaList).map(s -> "'" + s + "'").collect(Collectors.joining(",")));
        Map<Integer,TableId> tableIdMap = new HashMap<>();
        try (var statement = connection.prepareStatement(sql)) {
            var resultSet = statement.executeQuery();
            while (resultSet.next()) {
                int tableId = resultSet.getInt("OBJECT_ID");
                String tableName = resultSet.getString("TABLE_NAME");
                String schema = resultSet.getString("OWNER");
                tableIdMap.put(tableId, new TableId(tableName, schema));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tableIdMap;
    }

    private Map<TableId,TableMetadata> initMetadataMapFromSchema() {
        Map<TableId,TableMetadata> tableMetadataMap = new HashMap<>();
        var schemaList = config.getSchemaList().split(",");
        String sql = String.format(QUERY_COLUMN_ID, String.join(",", Arrays.stream(schemaList).map(s -> "'" + s + "'").collect(Collectors.toList())));
        try (var statement = connection.createStatement()) {
            String catalog = connection.getCatalog();
//            statement.execute("alter session set container= " + config.getDatabase());
            var resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String schema = resultSet.getString("OWNER");
                int columnId = resultSet.getInt("COLUMN_ID");
                String columnName = resultSet.getString("COLUMN_NAME");
                String dataType = resultSet.getString("DATA_TYPE");
                TableId tableId = new TableId(tableName, schema);
                if (!tableMetadataMap.containsKey(tableId)) {
                    tableMetadataMap.put(tableId, new TableMetadata(tableId, new HashMap<>()));
                }

                tableMetadataMap.get(tableId)
                        .getColumnIdMap().put(columnId, ColumnMetadataFactory.createColumnMeta(columnName, dataType));
            }

//            statement.execute("alter session set container= " + catalog);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tableMetadataMap;
    }

    private Map<Integer,TableId> initTableIdMapFromTable() {
        throw new UnsupportedOperationException("initTableIdMapFromTable not implemented");
    }

    private Map<TableId,TableMetadata> initMetadataMapFromTable() {
        throw new UnsupportedOperationException("initMetadataMapFromTable not implemented");
    }
}
