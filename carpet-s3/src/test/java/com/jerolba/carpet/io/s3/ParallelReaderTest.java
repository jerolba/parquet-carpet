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

import static com.jerolba.carpet.io.s3.Cancellation.NEVER_CANCELLED;
import static com.jerolba.carpet.io.s3.ParallelReader.CHUNK_SIZE;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.largeVirtualReader;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ParallelReaderTest {

    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    @AfterEach
    void shutdown() {
        executor.shutdown();
    }

    /**
     * Reads {@code length} bytes at {@code offset} of a virtual file where the
     * byte at position {@code pos} is {@code (byte) (pos % 256)}.
     */
    private ByteBuffer readVirtual(long offset, int length) throws Exception {
        ParallelReader reader = new ParallelReader(largeVirtualReader(offset + length), executor);
        ByteBuffer target = ByteBuffer.allocate(length);
        reader.readChunked(offset, target, NEVER_CANCELLED).get();
        return target;
    }

    private static void assertByteAt(ByteBuffer buffer, int index, long fileOffset) {
        assertEquals((byte) ((fileOffset + index) % 256), buffer.get(index), "Mismatch at index " + index);
    }

    // -------------------------------------------------------------------------
    // Splitting into chunks
    // -------------------------------------------------------------------------

    @Nested
    class Chunking {

        @Test
        void readsSlightlyMoreThanOneChunk() throws Exception {
            ByteBuffer result = readVirtual(0, CHUNK_SIZE + 1);

            assertByteAt(result, 0, 0);
            assertByteAt(result, CHUNK_SIZE - 1, 0);
            assertByteAt(result, CHUNK_SIZE, 0);
        }

        @Test
        void readsSeveralFullChunks() throws Exception {
            ByteBuffer result = readVirtual(0, CHUNK_SIZE * 3);

            for (int chunk = 0; chunk < 3; chunk++) {
                assertByteAt(result, chunk * CHUNK_SIZE, 0);
                assertByteAt(result, (chunk + 1) * CHUNK_SIZE - 1, 0);
            }
        }

        @Test
        void readsPartialLastChunk() throws Exception {
            int length = CHUNK_SIZE * 2 + CHUNK_SIZE / 2;
            ByteBuffer result = readVirtual(0, length);

            assertByteAt(result, CHUNK_SIZE - 1, 0);
            assertByteAt(result, CHUNK_SIZE, 0);
            assertByteAt(result, length - 1, 0);
        }

        @Test
        void readsEachChunkAtItsFileOffset() throws Exception {
            long offset = 1000;
            ByteBuffer result = readVirtual(offset, CHUNK_SIZE + 100);

            for (int i = 0; i < 100; i++) {
                assertByteAt(result, i, offset);
                assertByteAt(result, CHUNK_SIZE + i, offset);
            }
        }

        @Test
        void allBytesAreCorrect() throws Exception {
            int length = CHUNK_SIZE + 4096;
            ByteBuffer result = readVirtual(0, length);

            for (int i = 0; i < length; i++) {
                assertByteAt(result, i, 0);
            }
        }

        @Test
        void allBytesAreCorrectAtFileOffset() throws Exception {
            long offset = 500;
            int length = CHUNK_SIZE * 2 + 1000;
            ByteBuffer result = readVirtual(offset, length);

            for (int i = 0; i < length; i += 1024) {
                assertByteAt(result, i, offset);
            }
            assertByteAt(result, length - 1, offset);
        }

        @Test
        void chunksAreReadInParallel() throws Exception {
            // Each chunk read waits for the other two before going on, so the read can
            // only complete if all three chunks are in flight at the same time
            CyclicBarrier allChunksInFlight = new CyclicBarrier(3);
            AtomicInteger inFlight = new AtomicInteger();
            AtomicInteger maxInFlight = new AtomicInteger();
            SeekableReader reader = withHook(largeVirtualReader(CHUNK_SIZE * 3L), () -> {
                maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                try {
                    allChunksInFlight.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new IOException("Chunks were not read in parallel", e);
                }
                inFlight.decrementAndGet();
            });

            new ParallelReader(reader, executor)
                    .readChunked(0, ByteBuffer.allocate(CHUNK_SIZE * 3), NEVER_CANCELLED).get();

            assertEquals(3, maxInFlight.get(), "all chunks are read at the same time");
        }
    }

    // -------------------------------------------------------------------------
    // Target buffer contract
    // -------------------------------------------------------------------------

    @Nested
    class TargetBuffer {

        @Test
        void fillsOnlyTheRegionBetweenPositionAndLimit() throws Exception {
            byte untouched = (byte) 0xAB;
            int start = 100;
            int length = CHUNK_SIZE + 1000;
            long offset = 300;
            ByteBuffer target = ByteBuffer.allocate(start + length + 100);
            for (int i = 0; i < target.capacity(); i++) {
                target.put(i, untouched);
            }
            target.position(start).limit(start + length);

            new ParallelReader(largeVirtualReader(offset + length), executor)
                    .readChunked(offset, target, NEVER_CANCELLED).get();

            assertEquals(start, target.position(), "position must not move");
            assertEquals(start + length, target.limit(), "limit must not move");
            // Absolute gets are bounded by the limit, inspect the whole backing buffer
            ByteBuffer all = target.duplicate().clear();
            for (int i = 0; i < start; i++) {
                assertEquals(untouched, all.get(i), "Byte before the region written at " + i);
            }
            for (int i = 0; i < length; i += 1024) {
                assertEquals((byte) ((offset + i) % 256), all.get(start + i), "Mismatch at region index " + i);
            }
            assertEquals((byte) ((offset + length - 1) % 256), all.get(start + length - 1));
            for (int i = start + length; i < all.capacity(); i++) {
                assertEquals(untouched, all.get(i), "Byte after the region written at " + i);
            }
        }

        @Test
        void emptyTargetCompletesWithoutReading() throws Exception {
            AtomicInteger reads = new AtomicInteger();
            SeekableReader counting = withHook(largeVirtualReader(100), reads::incrementAndGet);

            new ParallelReader(counting, executor).readChunked(0, ByteBuffer.allocate(0), NEVER_CANCELLED).get();

            assertEquals(0, reads.get());
        }
    }

    // -------------------------------------------------------------------------
    // Failures and cancellation
    // -------------------------------------------------------------------------

    @Nested
    class FailuresAndCancellation {

        @Test
        void ioExceptionInAChunkFailsTheFuture() {
            SeekableReader failing = withHook(largeVirtualReader(CHUNK_SIZE * 2L), () -> {
                throw new IOException("Simulated chunk read failure");
            });

            ExecutionException ex = assertThrows(ExecutionException.class,
                    () -> new ParallelReader(failing, executor)
                            .readChunked(0, ByteBuffer.allocate(CHUNK_SIZE + 1), NEVER_CANCELLED).get());
            assertInstanceOf(IOException.class, ex.getCause());
        }

        @Test
        void alreadyCancelledReadsNothing() {
            AtomicInteger reads = new AtomicInteger();
            SeekableReader counting = withHook(largeVirtualReader(CHUNK_SIZE * 2L), reads::incrementAndGet);

            ExecutionException ex = assertThrows(ExecutionException.class,
                    () -> new ParallelReader(counting, executor)
                            .readChunked(0, ByteBuffer.allocate(CHUNK_SIZE + 1), () -> true).get());
            assertInstanceOf(CancellationException.class, ex.getCause());
            assertEquals(0, reads.get());
        }

        @Test
        void cancellingSkipsChunksNotYetStarted() {
            // A single thread runs the chunks one after another, so the flag raised by
            // the first read is seen by the rest
            ExecutorService sequential = Executors.newSingleThreadExecutor();
            try {
                AtomicBoolean cancelled = new AtomicBoolean(false);
                AtomicInteger reads = new AtomicInteger();
                SeekableReader cancelAfterFirst = withHook(largeVirtualReader(CHUNK_SIZE * 3L), () -> {
                    reads.incrementAndGet();
                    cancelled.set(true);
                });

                ExecutionException ex = assertThrows(ExecutionException.class,
                        () -> new ParallelReader(cancelAfterFirst, sequential)
                                .readChunked(0, ByteBuffer.allocate(CHUNK_SIZE * 3), cancelled::get).get());
                assertInstanceOf(CancellationException.class, ex.getCause());
                assertEquals(1, reads.get());
            } finally {
                sequential.shutdown();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    @FunctionalInterface
    private interface ReadHook {
        void beforeRead() throws IOException;
    }

    /**
     * Wraps a reader so that {@code hook} runs before every read.
     */
    private static SeekableReader withHook(SeekableReader target, ReadHook hook) {
        return new SeekableReader() {
            @Override
            public long getLength() throws IOException {
                return target.getLength();
            }

            @Override
            public int readFully(long pos, byte[] b, int off, int len) throws IOException {
                hook.beforeRead();
                return target.readFully(pos, b, off, len);
            }

            @Override
            public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
                hook.beforeRead();
                return target.readFully(pos, byteBuffer);
            }

            @Override
            public void close() throws IOException {
                target.close();
            }
        };
    }

}
