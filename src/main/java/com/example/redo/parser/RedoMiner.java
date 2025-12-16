package com.example.redo.parser;

import com.example.redo.config.Config;
import com.example.redo.deserialize.Deserializer;
import com.example.redo.deserialize.RBA;
import com.example.redo.metadata.Checker;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;

@Slf4j
public class RedoMiner {
    private RedoParser redoParser;
    private Deserializer deserializer;
    private Config config;
    private Connection connection;

    private PreparedStatement preparedStatement;
    private static final String SQL = "select CURRENT_SCN,\n" +
                "       L.SEQUENCE#, F.MEMBER, L.BLOCKSIZE, L.BYTES\n" +
                "from   V$DATABASE D, V$LOG L, V$LOGFILE F\n" +
                "where  L.STATUS = 'CURRENT'\n" +
                "  and  L.GROUP# = F.GROUP#\n" +
                "  and  L.THREAD#=1\n" +
                "  and  F.STATUS is null\n" +
                "  and  rownum = 1";
    private Checker checker;

    public RedoMiner(Config config, Deserializer deserializer, Checker checker) {
        this.redoParser = new RedoParser(0, deserializer,checker);
        this.config = config;
        this.checker = checker;
    }

    private void initConnection() throws SQLException {
        connection = DriverManager.getConnection(
            config.getUrl(),
            config.getUser(),
            config.getPassword()
        );
        preparedStatement = connection.prepareStatement(SQL);
    }

    /**
     * 解析redo日志文件
     */
    public void parseRedoFile() {
        try {
            initConnection();
        } catch (SQLException e) {
            throw new RuntimeException("数据库连接初始化失败", e);
        }
        while (true) {
            try {
                CurrentRedo currentRedo = getCurrentRedo();
                log.info("current redo file: {}", currentRedo.getMember());
                if (redoParser.getDba() == null) {

                    redoParser.parse(Path.of(currentRedo.getMember()));
                }else {
                    redoParser.parse(Path.of(currentRedo.getMember()), redoParser.getDba());
                }
                log.info("redo file {} parsed", currentRedo.getMember());
                Thread.sleep(3000);
            } catch (SQLException e) {
                throw new RuntimeException("查询当前日志文件失败", e);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private CurrentRedo getCurrentRedo() throws SQLException {
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next()) {
            throw new RuntimeException("查询当前日志文件失败");
        }else {
            String member = resultSet.getString("MEMBER");
            member = member.replace(config.getOriginPath(), config.getTargetPath());
            return new CurrentRedo(
                resultSet.getLong("CURRENT_SCN"),
                resultSet.getInt("SEQUENCE#"),
                    member,
                resultSet.getInt("BLOCKSIZE"),
                resultSet.getLong("BYTES")
            );
        }
    }

    public static class CurrentRedo {
        private long currentScn;
        private int sequence;
        private String member;
        private int blockSize;
        private long bytes;
        public CurrentRedo(long currentScn, int sequence, String member, int blockSize, long bytes) {
            this.currentScn = currentScn;
            this.sequence = sequence;
            this.member = member;
            this.blockSize = blockSize;
            this.bytes = bytes;
        }

        public String getMember() {
            return member;
        }

    }
}
