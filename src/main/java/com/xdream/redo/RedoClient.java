package com.xdream.redo;

import com.xdream.redo.config.Config;
import com.xdream.redo.deserialize.RecordDeserializer;
import com.xdream.redo.deserialize.RedoEvent;
import com.xdream.redo.metadata.MetadataManager;
import com.xdream.redo.parser.RedoMiner;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedoClient {
    private Config config;
    private MetadataManager  metadataManager;
    private ArrayBlockingQueue<RedoEvent> redoEventList;
    private RecordDeserializer recordDeserializer;
    private volatile Throwable throwable = null;
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
        executorService.submit(() ->{
            try {
                RedoMiner redoMiner = new RedoMiner(config, recordDeserializer,metadataManager.getChecker());
                redoMiner.parseRedoFile();
            } catch (Exception e) {
                log.error("parseRedoFile error", e);
                throwable = e;
            }
        });
    }

    public void stop() {
        executorService.shutdownNow();
    }

    public RedoEvent redoEvent() {
        if (throwable != null) {
            throw new RuntimeException(throwable);
        }
        try {
            return redoEventList.poll(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public RedoEvent getRedoEvent() {
        if (throwable != null) {
            throw new RuntimeException(throwable);
        }
        try {
            return redoEventList.poll(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
