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

public class RedoClient {
    private Config config;
    private MetadataManager  metadataManager;
    private ArrayBlockingQueue<RedoEvent> redoEventList;
    private RecordDeserializer recordDeserializer;
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
        RedoMiner redoMiner = new RedoMiner(config, recordDeserializer);
        redoMiner.parseRedoFile();
    }

    public RedoEvent getRedoEvent() {
        try {
            return redoEventList.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
