package com.jerolba.carpet.io.s3;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OwnedExecutor implements Executor {

    private static final Logger logger = LoggerFactory.getLogger(OwnedExecutor.class);

    private final ExecutorService executorService;

    public OwnedExecutor(int concurrency) {
        this.executorService = Executors.newFixedThreadPool(concurrency);
    }

    @Override
    public void execute(Runnable command) {
        executorService.execute(command);
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Executor did not terminate within timeout, forcing shutdown");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

}