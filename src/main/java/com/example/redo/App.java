package com.example.redo;

import com.example.redo.model.RedoParseResult;
import com.example.redo.parser.RedoParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(
        name = "redo-tool",
        mixinStandardHelpOptions = true,
        version = "0.1.0",
        description = "Oracle redo 日志解析工具",
        subcommands = {App.ParseCommand.class}
)
public class App implements Runnable {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    @Command(
            name = "parse",
            description = "解析 redo 日志，输出包含 DML/DDL 的 JSON"
    )
    static class ParseCommand implements Callable<Integer> {

        @Parameters(index = "0", paramLabel = "FILE", description = "redo 日志路径")
        private Path file;

        @Option(names = {"-l", "--limit-records"}, description = "最多解析的 record 数量（0 表示全部）")
        private int recordLimit = 0;

        @Override
        public Integer call() throws Exception {
            if (!Files.isRegularFile(file)) {
                System.err.println("文件不存在: " + file);
                return 2;
            }

            RedoParser parser = new RedoParser(recordLimit);
            RedoParseResult result = parser.parse(file);

            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .enable(SerializationFeature.INDENT_OUTPUT);

            mapper.writeValue(System.out, result);
            return 0;
        }
    }
}

