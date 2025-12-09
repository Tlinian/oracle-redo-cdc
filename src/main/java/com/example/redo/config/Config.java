package com.example.redo.config;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

@Getter
public class Config {
    private static final String REDO_FILE_PATH_KEY = "redo.file.path";
    private static final String REDO_FILE_PATH_DEFAULT = "redo.filename";
    private static final String START_SCN_KEY = "start.scn";
    private static final String URL_KEY = "url";
    private static final String USER_KEY = "user";
    private static final String PASSWORD_KEY = "password";
    private static final String DATABASE_KEY = "database";
    private static final String SCHEMA_LIST_KEY = "schema.list";
    private static final String MINER_MODE_KEY = "miner.mode";

    private String redoFileName;
    private long startScn;
    private String url;
    private String user;
    private String password;
    private String database;
    private String schemaList;
    private String minerMode;
    
    /**
     * 从配置文件读取配置
     * @param configPath 配置文件路径
     * @throws IOException 配置文件读取异常
     */
    public Config(Path configPath) throws IOException {
        Properties properties = new Properties();
        
        try (InputStream inputStream = Files.newInputStream(configPath)) {
            properties.load(inputStream);
            
            redoFileName = properties.getProperty(REDO_FILE_PATH_DEFAULT);
            startScn = Long.parseLong(properties.getProperty(START_SCN_KEY));
            url = properties.getProperty(URL_KEY);
            user = properties.getProperty(USER_KEY);
            password = properties.getProperty(PASSWORD_KEY);
            database = properties.getProperty(DATABASE_KEY);
            schemaList = properties.getProperty(SCHEMA_LIST_KEY);
            minerMode = properties.getProperty(MINER_MODE_KEY);
        }
    }

}
