package com.example.redo;

import com.example.redo.config.Config;
import com.example.redo.deserialize.PrintDeserializer;
import com.example.redo.deserialize.RecordDeserializer;
import com.example.redo.deserialize.RedoEvent;
import com.example.redo.metadata.MetadataManager;
import com.example.redo.model.RedoParseResult;
import com.example.redo.parser.RedoParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;

@Command(
        name = "redo-tool",
        mixinStandardHelpOptions = true,
        version = "0.1.0",
        description = "Oracle redo 日志解析工具"
)
public class App implements Callable<Integer> {

    @Option(names = {"--parse-path"}, paramLabel = "CONFIG_PATH", description = "指定配置文件路径进行解析")
    private Path parsePath;

    @Option(names = {"-l", "--limit-records"}, description = "最多解析的 record 数量（0 表示全部）")
    private int recordLimit = 0;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {

        if (parsePath == null) {
            System.err.println("错误：必须提供 --parse-path 选项");
            CommandLine.usage(this, System.out);
            return 2;
        }

        if (!Files.isRegularFile(parsePath)) {
            System.err.println("错误：配置文件不存在或不是常规文件: " + parsePath);
            return 2;
        }
        Config config = new Config(parsePath);
        System.out.println("配置信息: " + config);
        if (config.getMinerMode().equalsIgnoreCase("file")) {
            // 处理单个文件解析
            Path redoFilePath = Paths.get(config.getRedoFileName());
            if (!Files.isRegularFile(redoFilePath)) {
                System.err.println("错误：文件不存在或不是常规文件: " + redoFilePath);
                return 2;
            }
            ArrayBlockingQueue<RedoEvent> redoEventList = new ArrayBlockingQueue<>(1000);
            MetadataManager metadataManager = new MetadataManager(config);
            metadataManager.init();
            RecordDeserializer recordDeserializer = new RecordDeserializer(config, metadataManager, redoEventList);
            System.out.println("开始解析文件: " + redoFilePath);
            System.out.println("解析的起始点: " + config.getStartScn());
            System.out.println("如果需要解析所有记录，请设置startScn为0");
            RedoParser parser = new RedoParser(recordLimit, recordDeserializer,metadataManager.getChecker(),config.getStartScn());
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            executorService.submit(() -> {
                try {
                    RedoParseResult result = parser.parse(redoFilePath);
                    System.out.println("解析完成，共解析 " + result.records().size() + " 条记录");
                    countDownLatch.countDown();
                } catch (Exception e) {
                    System.err.println("解析文件时出错: " + e.getMessage());
                }
            });
            while (countDownLatch.getCount() != 0) {
                RedoEvent e = redoEventList.poll();
                if (e != null) {
                    System.out.println("解析到的 redo 事件: " + e);
                }
            }
            System.out.println("解析完成");
        } else if (parsePath != null) {
            // 处理配置文件解析
            RedoClient client = new RedoClient(config);
            client.init();
            new Thread(client::start).start();
            while (true) {
                RedoEvent e = client.getRedoEvent();
                if (e != null) {
                    System.out.println(e.toString());
                }else {
                    System.out.println("等待新的 redo 事件...");
                    Thread.sleep(3000);
                }
            }
        }

        return 0;
    }
}

