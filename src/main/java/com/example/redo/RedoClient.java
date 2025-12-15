package com.example.redo;

import com.example.redo.config.Config;
import com.example.redo.deserialize.RecordDeserializer;
import com.example.redo.deserialize.RedoEvent;
import com.example.redo.metadata.MetadataManager;
import com.example.redo.parser.RedoMiner;
import com.example.redo.parser.RedoParser;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RedoClient {
    private Config config;
    private MetadataManager  metadataManager;
    private ArrayBlockingQueue<RedoEvent> redoEventList;
    private RecordDeserializer recordDeserializer;
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    public RedoClient(Config config) {
        this.config = config;
        this.metadataManager = new MetadataManager(config);
        this.redoEventList = new ArrayBlockingQueue<>(1000);
    }

    public void init() {
        metadataManager.init();
        recordDeserializer = new RecordDeserializer(config,metadataManager,redoEventList);
    }

    public void start() {
        String redoFileName = config.getRedoFileName();
        executorService.submit(() ->{
            RedoMiner redoMiner = new RedoMiner(config, recordDeserializer,metadataManager.getChecker());
            redoMiner.parseRedoFile();
        });
    }

    public void stop() {
        executorService.shutdownNow();
    }

    public RedoEvent redoEvent() {
        try {
            return redoEventList.poll(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public RedoEvent getRedoEvent() {
        try {
            return redoEventList.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
