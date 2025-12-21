package com.xdream.redo.parser;

import com.xdream.redo.config.Config;
import com.xdream.redo.deserialize.Deserializer;
import com.xdream.redo.metadata.Checker;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class RedoMiner {
    private RedoParser redoParser;
    private Deserializer deserializer;
    private Config config;
    private Connection connection;

    private PreparedStatement preparedStatement;
    private PreparedStatement archiveStatement;
    private PreparedStatement onlineStatement;
    private static final String SQL = "select CURRENT_SCN,\n" +
                "       L.SEQUENCE#, F.MEMBER, L.BLOCKSIZE, L.BYTES\n" +
                "from   V$DATABASE D, V$LOG L, V$LOGFILE F\n" +
                "where  L.STATUS = 'CURRENT'\n" +
                "  and  L.GROUP# = F.GROUP#\n" +
                "  and  L.THREAD#=1\n" +
                "  and  F.STATUS is null\n" +
                "  and  rownum = 1";

    private static final String SQL_ONLINE = """
select CURRENT_SCN,
       L.SEQUENCE#, F.MEMBER, L.BLOCKSIZE, L.BYTES
from   V$DATABASE D, V$LOG L, V$LOGFILE F
where  L.STATUS = 'CURRENT'
  and  L.GROUP# = F.GROUP#
  and  L.THREAD#=1
  and  F.STATUS is null
  and  rownum = 1 and FIRST_CHANGE# <= ?
""";

    private static final String SQL_ARCHIVE = """
select NAME,SEQUENCE#,BLOCKS,BLOCK_SIZE from SYS.V_$ARCHIVED_LOG where NEXT_CHANGE# > ? and SEQUENCE# > ? order by SEQUENCE#
""";
    private Checker checker;

    public RedoMiner(Config config, Deserializer deserializer, Checker checker) {
        this.redoParser = new RedoParser(0, deserializer,checker,config.getStartScn());
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
        archiveStatement = connection.prepareStatement(SQL_ARCHIVE);
        onlineStatement = connection.prepareStatement(SQL_ONLINE);
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
        CurrentRedo lastRedo = null;
        long lastScn = config.getStartScn();
        while (true) {
            try {
                if (lastRedo!=null && !lastRedo.isOnline){
                    lastScn = lastRedo.getNextScn();
                }

                List<CurrentRedo> currentRedoList = getCurrentRedoList(lastScn,lastRedo != null ? (
                    lastRedo.isOnline ? lastRedo.sequence-- : lastRedo.sequence
                ):0);
                for (int i = 0; i < currentRedoList.size(); i++) {
                    CurrentRedo currentRedo = currentRedoList.get(i);
                    if (lastRedo != null && !lastRedo.isOnline && lastRedo.sequence == currentRedo.sequence){
                        continue;
                    }
                    lastRedo = currentRedo;
                    System.out.println("current redo file: " + currentRedo);
                    log.info("current redo file: {}", currentRedo.getMember());
                    if (redoParser.getDba() == null) {

                        redoParser.parse(Path.of(currentRedo.getMember()),currentRedo);
                    }else {
                        redoParser.parse(Path.of(currentRedo.getMember()), redoParser.getDba(),currentRedo);
                    }
                    lastScn = redoParser.getLastScn();
                    log.info("redo file {} parsed", currentRedo.getMember());
                }
                Thread.sleep(3000);
            } catch (SQLException e) {
                throw new RuntimeException("查询当前日志文件失败", e);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<CurrentRedo> getCurrentRedoList(long Scn,int seq) throws SQLException {
        // 先查询归档
        archiveStatement.setLong(1, Scn);
        archiveStatement.setInt(2, seq);
        try (ResultSet resultSet = archiveStatement.executeQuery()) {
            List<CurrentRedo> currentRedoList = new ArrayList<>();
            while (resultSet.next()) {
                String member = resultSet.getString("NAME");
                member = member.replace(config.getOriginPath(), config.getTargetPath());
                currentRedoList.add(new CurrentRedo(
                        Long.MAX_VALUE,
//                        resultSet.getLong("NEXT_CHANGE#"),
                        resultSet.getInt("SEQUENCE#"),
                        member,
                        resultSet.getInt("BLOCK_SIZE"),
                        resultSet.getLong("BLOCKS"),false
                ));
            }
            if (!currentRedoList.isEmpty()){
                return currentRedoList;
            }
        }


        // 再查询在线日志
        onlineStatement.setLong(1, Scn);
        try (ResultSet resultSet = onlineStatement.executeQuery()) {
            if (!resultSet.next()) {
                throw new RuntimeException("查询当前日志文件失败");
            }else {
                String member = resultSet.getString("MEMBER");
                member = member.replace(config.getOriginPath(), config.getTargetPath());
                int blocksize = resultSet.getInt("BLOCKSIZE");
                return Collections.singletonList(new CurrentRedo(
                        Long.MAX_VALUE,
                        resultSet.getInt("SEQUENCE#"),
                        member,
                        blocksize,
                        resultSet.getLong("BYTES")/blocksize, true
                ));
            }
        }
    }

    private CurrentRedo getCurrentRedo(long Scn) throws SQLException {
        // 先查询归档

        // 再查询在线日志
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
                resultSet.getLong("BYTES"),false
            );
        }
    }

    @ToString
    @AllArgsConstructor
    @Getter
    public static class CurrentRedo {
        private long nextScn;
        private int sequence;
        private String member;
        private int blockSize;
        private long blockCount;
        private boolean isOnline;

        public String getMember() {
            return member;
        }

    }
}
