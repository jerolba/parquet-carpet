/**
 * Copyright 2023 Jerónimo López Bezanilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jerolba.carpet.io.s3;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CustomExecutor is an abstraction over Java's ExecutorService that attempts to
 * use virtual threads if they are available (Java 19+), while providing a
 * fallback implementation using a fixed thread pool for older Java versions.
 *
 * Closing a file, if existing Executor is a CustomExecutor, we know that it was
 * not provided by the user but was created by Carpet and must be explicitly
 * destroyed.
 */
interface CustomExecutor extends Executor {

    static final Logger logger = LoggerFactory.getLogger(CustomExecutor.class);

    void shutdown();

    static CustomExecutor createCustomExecutor(int concurrency) {
        try {
            return new LimitedVirtualExecutor(concurrency);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return new FixedThreadPool(concurrency);
        }
    }

    static Executor createVirtualThreadExecutorWithCommonPoolFallback(int concurrency) {
        try {
            return new LimitedVirtualExecutor(concurrency);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return ForkJoinPool.commonPool();
        }
    }

    /**
     * Implementation using virtual threads with a semaphore to limit concurrency.
     * This allows us to take advantage of virtual threads while still controlling
     * the number of concurrent tasks.
     */
    class LimitedVirtualExecutor implements CustomExecutor {

        private final ExecutorService delegate;
        private final Semaphore semaphore;

        public LimitedVirtualExecutor(int maxConcurrency)
                throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException,
                InvocationTargetException {
            Method factoryMethod = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            this.delegate = (ExecutorService) factoryMethod.invoke(null);
            this.semaphore = new Semaphore(maxConcurrency);
            logger.debug("Created LimitedVirtualExecutor with max concurrency: {}", maxConcurrency);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(() -> {
                try {
                    semaphore.acquire();
                    try {
                        command.run();
                    } finally {
                        semaphore.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        @Override
        public void shutdown() {
            CustomExecutor.delegateShutdown(delegate);
        }
    }

    /**
     * Fallback implementation using a fixed thread pool if virtual threads are not
     * available.
     */
    class FixedThreadPool implements CustomExecutor {

        private final ExecutorService executorService;

        public FixedThreadPool(int concurrency) {
            this.executorService = Executors.newFixedThreadPool(concurrency);
            logger.debug("Created FixedThreadPool with concurrency: {}", concurrency);
        }

        @Override
        public void execute(Runnable command) {
            executorService.execute(command);
        }

        @Override
        public void shutdown() {
            CustomExecutor.delegateShutdown(executorService);
        }
    }

    private static void delegateShutdown(ExecutorService delegate) {
        delegate.shutdown();
        try {
            if (!delegate.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("Executor did not terminate within timeout, forcing shutdown");
                delegate.shutdownNow();
            }
        } catch (InterruptedException e) {
            delegate.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

}