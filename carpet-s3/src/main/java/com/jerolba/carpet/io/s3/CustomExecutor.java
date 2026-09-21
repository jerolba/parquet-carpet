/**
 * Copyright 2026 Jerónimo López Bezanilla
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
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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

    private static Executor createVirtualThreadExecutorWithCommonPoolFallback(int concurrency) {
        try {
            return new LimitedVirtualExecutor(concurrency);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return ForkJoinPool.commonPool();
        }
    }

    /**
     * Supplies the executor of each new stream of a file, from the builder
     * configuration. Each stream gets its own executor, shut down when the stream
     * is closed; a user provided executor is shared and never shut down by Carpet.
     *
     * @param userExecutor       executor configured by the user, or null
     * @param concurrency        concurrency configured by the user, or null
     * @param defaultConcurrency concurrency when none is configured
     * @param sequential         executor for a concurrency of 1, or null if the
     *                           caller does not use an executor in that case
     * @return the executor supplier, or null for a concurrency of 1 without a
     *         sequential executor
     */
    static Supplier<Executor> executorFactory(Executor userExecutor, Integer concurrency, int defaultConcurrency,
            Executor sequential) {
        if (userExecutor != null) {
            return () -> userExecutor;
        }
        if (concurrency == null) {
            return () -> createVirtualThreadExecutorWithCommonPoolFallback(defaultConcurrency);
        }
        if (concurrency == 1) {
            return sequential == null ? null : () -> sequential;
        }
        int streamConcurrency = concurrency;
        return () -> createCustomExecutor(streamConcurrency);
    }

    /**
     * Implementation using virtual threads with a semaphore to limit concurrency.
     * This allows us to take advantage of virtual threads while still controlling
     * the number of concurrent tasks.
     */
    class LimitedVirtualExecutor implements CustomExecutor {

        private final ExecutorService delegate;
        private final Semaphore semaphore;

        LimitedVirtualExecutor(int maxConcurrency)
                throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException,
                InvocationTargetException {
            this(newVirtualThreadPerTaskExecutor(), maxConcurrency);
        }

        /**
         * @param delegate       executor that runs each task in its own thread, so a
         *                       task waiting for a permit does not block another one
         * @param maxConcurrency maximum number of tasks running at the same time
         */
        LimitedVirtualExecutor(ExecutorService delegate, int maxConcurrency) {
            this.delegate = delegate;
            this.semaphore = new Semaphore(maxConcurrency);
            logger.debug("Created LimitedVirtualExecutor with max concurrency: {}", maxConcurrency);
        }

        private static ExecutorService newVirtualThreadPerTaskExecutor()
                throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            Method factoryMethod = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) factoryMethod.invoke(null);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(() -> {
                boolean permitted = acquirePermit();
                try {
                    command.run();
                } finally {
                    if (permitted) {
                        semaphore.release();
                    }
                }
            });
        }

        /**
         * Waits for a permit. The wait is only interrupted by a forced shutdown; the
         * task then runs anyway, without a permit and with the interrupt flag set,
         * instead of being dropped: a dropped task leaves its future incomplete
         * forever, while a task run on an interrupted thread fails fast, because
         * cancelled reads fail before starting and the S3 SDK aborts a request issued
         * from an interrupted thread.
         *
         * @return whether a permit was acquired and must be released
         */
        private boolean acquirePermit() {
            try {
                semaphore.acquire();
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
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

        FixedThreadPool(int concurrency) {
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
                forceShutdown(delegate);
            }
        } catch (InterruptedException e) {
            forceShutdown(delegate);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Interrupts the running tasks. Tasks still queued, which only a thread pool
     * has, are dropped and their futures never complete: nothing waits on them
     * once a stream is closed, but it is logged in case something does.
     */
    private static void forceShutdown(ExecutorService delegate) {
        List<Runnable> dropped = delegate.shutdownNow();
        if (!dropped.isEmpty()) {
            logger.warn("{} queued tasks dropped on forced shutdown, their futures will not complete",
                    dropped.size());
        }
    }

}