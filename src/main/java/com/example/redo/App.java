package com.example.redo;

import com.example.redo.config.Config;
import com.example.redo.deserialize.PrintDeserializer;
import com.example.redo.deserialize.RecordDeserializer;
import com.example.redo.deserialize.RedoEvent;
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
import java.util.concurrent.Callable;

@Command(
        name = "redo-tool",
        mixinStandardHelpOptions = true,
        version = "0.1.0",
        description = "Oracle redo 日志解析工具"
)
public class App implements Callable<Integer> {

    @Option(names = {"--parse-file"}, paramLabel = "FILE", description = "指定单个 redo 日志文件进行解析")
    private Path parseFile;

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
        // 参数验证：确保两种启动方式的参数互斥且至少提供一种
        if (parseFile != null && parsePath != null) {
            System.err.println("错误：--parse-file 和 --parse-path 选项不能同时使用");
            return 2;
        }

        if (parseFile == null && parsePath == null) {
            System.err.println("错误：必须提供 --parse-file 或 --parse-path 选项");
            CommandLine.usage(this, System.out);
            return 2;
        }

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .enable(SerializationFeature.INDENT_OUTPUT);

        if (parseFile != null) {
            // 处理单个文件解析
            if (!Files.isRegularFile(parseFile)) {
                System.err.println("错误：文件不存在或不是常规文件: " + parseFile);
                return 2;
            }

            RedoParser parser = new RedoParser(recordLimit, new PrintDeserializer());
            RedoParseResult result = parser.parse(parseFile);
            mapper.writeValue(System.out, result);
        } else if (parsePath != null) {
            // 处理配置文件解析
            if (!Files.isRegularFile(parsePath)) {
                System.err.println("错误：配置文件不存在或不是常规文件: " + parsePath);
                return 2;
            }

            try {
                Config config = new Config(parsePath);
                // 这里可以从配置文件中读取更多配置
                Path redoFilePath = Paths.get(config.getRedoFileName());
                
                if (!Files.isRegularFile(redoFilePath)) {
                    System.err.println("错误：配置文件中指定的 redo 日志文件不存在: " + redoFilePath);
                    return 2;
                }

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
            } catch (IOException e) {
                System.err.println("错误：读取配置文件失败: " + e.getMessage());
                return 2;
            }
        }

        return 0;
    }
}

