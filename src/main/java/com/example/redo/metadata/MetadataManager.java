package com.example.redo.metadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MetadataManager {
    // URL, USER, PASSWORD
    private static final String URL = "jdbc:oracle:thin:@localhost:1521:ORCLCDB";
    private static final String USER = "system";
    private static final String PASSWORD = "oracle";

    Connection connection = null;
    public MetadataManager() {
        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Map<Integer,TableId> tableIdMap = new HashMap<>();

    private Map<Integer,TableId> TableMetadata = new HashMap<>();
}
