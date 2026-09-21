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

import static com.jerolba.carpet.io.s3.CustomExecutor.executorFactory;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

class CustomExecutorTest {

    private final Executor sequential = Runnable::run;

    @Test
    void userExecutorIsSharedByEveryStream() {
        Executor user = command -> {
        };
        Supplier<Executor> factory = executorFactory(user, null, 4, sequential);

        assertSame(user, factory.get());
        assertSame(user, factory.get());
    }

    @Test
    void concurrencyOfOneUsesTheSequentialExecutor() {
        assertSame(sequential, executorFactory(null, 1, 4, sequential).get());
    }

    @Test
    void concurrencyOfOneWithoutSequentialExecutorHasNoFactory() {
        assertNull(executorFactory(null, 1, 4, null));
    }

    @Test
    void configuredConcurrencyCreatesAnOwnedExecutorPerStream() {
        Supplier<Executor> factory = executorFactory(null, 3, 4, sequential);

        CustomExecutor first = assertInstanceOf(CustomExecutor.class, factory.get());
        CustomExecutor second = assertInstanceOf(CustomExecutor.class, factory.get());
        assertNotSame(first, second);
        first.shutdown();
        second.shutdown();
    }

    /**
     * A forced shutdown interrupts tasks waiting for a permit. They must still
     * run, so that their futures complete, and see the interrupt.
     */
    @Test
    void taskInterruptedWhileWaitingForAPermitStillRuns() throws Exception {
        ExecutorService threads = Executors.newCachedThreadPool();
        var executor = new CustomExecutor.LimitedVirtualExecutor(threads, 1);
        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> holder = CompletableFuture.runAsync(() -> {
            holding.countDown();
            awaitUninterruptibly(release); // keeps the only permit through the interrupt
        }, executor);
        assertTrue(holding.await(5, TimeUnit.SECONDS));
        AtomicBoolean interrupted = new AtomicBoolean();
        CompletableFuture<Void> waiting = CompletableFuture.runAsync(
                () -> interrupted.set(Thread.currentThread().isInterrupted()), executor);

        threads.shutdownNow();

        waiting.get(5, TimeUnit.SECONDS);
        assertTrue(interrupted.get(), "the task runs with the interrupt flag set");
        release.countDown();
        holder.get(5, TimeUnit.SECONDS);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void defaultConcurrencyCreatesAnExecutorPerStream() {
        Supplier<Executor> factory = executorFactory(null, null, 4, sequential);

        Executor first = factory.get();
        Executor second = factory.get();
        if (first instanceof CustomExecutor owned) {
            // Virtual threads available: one owned executor per stream
            assertNotSame(first, second);
            owned.shutdown();
            ((CustomExecutor) second).shutdown();
        } else {
            // Fallback to the common pool, shared and never shut down
            assertSame(first, second);
        }
    }

}
