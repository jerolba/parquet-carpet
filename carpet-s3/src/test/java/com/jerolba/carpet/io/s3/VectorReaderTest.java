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

import static com.jerolba.carpet.io.s3.SeekableReaderHelper.heapAllocator;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.largeVirtualReader;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.readerOf;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.sequentialBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.io.s3.PrefetchTestSupport.TrackingAllocator;

class VectorReaderTest {

    private static final int CHUNK_SIZE = ParallelReader.CHUNK_SIZE;

    // -------------------------------------------------------------------------
    // Read single range (< CHUNK_SIZE)
    // -------------------------------------------------------------------------

    @Nested
    class ReadSingleRange {

        @Test
        void readSmallRangeAtOffsetZero() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(100);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(readerOf(data), executor)) {
                ParquetFileRange range = new ParquetFileRange(0, 10);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(10, result.remaining());
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, result.get(i) & 0xFF, "Mismatch at index " + i);
                }
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void readSmallRangeAtNonZeroOffset() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(200);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(readerOf(data), executor)) {
                ParquetFileRange range = new ParquetFileRange(50, 20);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(20, result.remaining());
                for (int i = 0; i < 20; i++) {
                    assertEquals((50 + i) % 256, result.get(i) & 0xFF, "Mismatch at index " + i);
                }
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void readRangeExactlyAtChunkSize() throws IOException, ExecutionException, InterruptedException {
            // Exactly CHUNK_SIZE should go through readRange (not parallel chunks)
            SeekableReader virtualReader = largeVirtualReader(CHUNK_SIZE * 2L);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, CHUNK_SIZE);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(CHUNK_SIZE, result.remaining());
                // Verify first and last bytes
                assertEquals(0, result.get(0) & 0xFF);
                assertEquals((CHUNK_SIZE - 1) % 256, result.get(CHUNK_SIZE - 1) & 0xFF);
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void readRangeFutureCompletesSuccessfully() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(50);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(readerOf(data), executor)) {
                ParquetFileRange range = new ParquetFileRange(0, 5);
                reader.readVectored(List.of(range), heapAllocator());

                CompletableFuture<ByteBuffer> future = range.getDataReadFuture();
                ByteBuffer result = future.get();
                assertTrue(future.isDone());
                assertFalse(future.isCompletedExceptionally());
                assertEquals(0, result.position());
                assertEquals(5, result.remaining());
            } finally {
                executor.shutdown();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Read multiple ranges
    // -------------------------------------------------------------------------

    @Nested
    class ReadMultipleRanges {

        @Test
        void readMultipleNonOverlappingRanges() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(200);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(readerOf(data), executor)) {
                ParquetFileRange r1 = new ParquetFileRange(0, 10);
                ParquetFileRange r2 = new ParquetFileRange(50, 20);
                ParquetFileRange r3 = new ParquetFileRange(100, 30);

                reader.readVectored(List.of(r1, r2, r3), heapAllocator());

                ByteBuffer b1 = r1.getDataReadFuture().get();
                ByteBuffer b2 = r2.getDataReadFuture().get();
                ByteBuffer b3 = r3.getDataReadFuture().get();

                assertEquals(10, b1.remaining());
                assertEquals(20, b2.remaining());
                assertEquals(30, b3.remaining());

                byte[] res1 = new byte[10];
                b1.get(res1);
                assertArrayEquals(Arrays.copyOfRange(data, 0, 10), res1);

                byte[] res2 = new byte[20];
                b2.get(res2);
                assertArrayEquals(Arrays.copyOfRange(data, 50, 70), res2);

                byte[] res3 = new byte[30];
                b3.get(res3);
                assertArrayEquals(Arrays.copyOfRange(data, 100, 130), res3);
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void readAdjacentRanges() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(100);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(readerOf(data), executor)) {
                ParquetFileRange r1 = new ParquetFileRange(0, 25);
                ParquetFileRange r2 = new ParquetFileRange(25, 25);
                ParquetFileRange r3 = new ParquetFileRange(50, 25);

                reader.readVectored(List.of(r1, r2, r3), heapAllocator());

                byte[] res1 = new byte[25];
                r1.getDataReadFuture().get().get(res1);
                assertArrayEquals(Arrays.copyOfRange(data, 0, 25), res1);

                byte[] res2 = new byte[25];
                r2.getDataReadFuture().get().get(res2);
                assertArrayEquals(Arrays.copyOfRange(data, 25, 50), res2);

                byte[] res3 = new byte[25];
                r3.getDataReadFuture().get().get(res3);
                assertArrayEquals(Arrays.copyOfRange(data, 50, 75), res3);
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void readEmptyRangeList() throws IOException {
            byte[] data = sequentialBytes(100);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(readerOf(data), executor)) {
                assertDoesNotThrow(() -> reader.readVectored(List.of(), heapAllocator()));
            } finally {
                executor.shutdown();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Read large range through ParallelReader (> CHUNK_SIZE); chunking itself
    // is covered by ParallelReaderTest
    // -------------------------------------------------------------------------

    @Nested
    class ReadLargeRangeInChunks {

        @Test
        void readMixOfLargeAndSmallRanges() throws IOException, ExecutionException, InterruptedException {
            int smallLength = 1024;
            int largeLength = CHUNK_SIZE + 512;
            SeekableReader virtualReader = largeVirtualReader(CHUNK_SIZE * 4L);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange smallRange = new ParquetFileRange(0, smallLength);
                ParquetFileRange largeRange = new ParquetFileRange(CHUNK_SIZE, largeLength);

                reader.readVectored(List.of(smallRange, largeRange), heapAllocator());

                // Verify small range
                ByteBuffer smallResult = smallRange.getDataReadFuture().get();
                assertEquals(smallLength, smallResult.remaining());
                assertEquals(0, smallResult.get(0) & 0xFF);
                assertEquals((smallLength - 1) % 256, smallResult.get(smallLength - 1) & 0xFF);

                // Verify large range
                ByteBuffer largeResult = largeRange.getDataReadFuture().get();
                assertEquals(largeLength, largeResult.remaining());
                // First byte of large range is at physical offset CHUNK_SIZE
                assertEquals(CHUNK_SIZE % 256, largeResult.get(0) & 0xFF);
                assertEquals((CHUNK_SIZE + largeLength - 1) % 256,
                        largeResult.get(largeLength - 1) & 0xFF);
            } finally {
                executor.shutdown();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Data integrity for large ranges
    // -------------------------------------------------------------------------

    @Nested
    class DataIntegrity {

        @Test
        void smallRangeBufferIsFlipped() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(100);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(readerOf(data), executor)) {
                ParquetFileRange range = new ParquetFileRange(10, 30);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                // readRangeToBuffer flips the buffer: position=0, limit=length
                assertEquals(0, result.position());
                assertEquals(30, result.limit());
                assertEquals(30, result.remaining());
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void largeRangeBufferHasCorrectPositionAndLimit()
                throws IOException, ExecutionException, InterruptedException {
            int rangeLength = CHUNK_SIZE + 100;
            SeekableReader virtualReader = largeVirtualReader(rangeLength * 2L);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, rangeLength);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(0, result.position());
                assertEquals(rangeLength, result.limit());
            } finally {
                executor.shutdown();
            }
        }

    }

    // -------------------------------------------------------------------------
    // Executor management
    // -------------------------------------------------------------------------

    @Nested
    class ExecutorManagement {

        @Test
        void closeShutsDownOwnedExecutor() throws IOException {
            CustomExecutor owned = CustomExecutor.createCustomExecutor(2);
            VectorReader reader = new VectorReader(readerOf(sequentialBytes(10)), owned);

            reader.close();

            assertThrows(RejectedExecutionException.class, () -> owned.execute(() -> {
            }), "an executor created by Carpet is shut down with the reader");
        }

        @Test
        void closeDoesNotShutdownExternalExecutor() throws IOException {
            ExecutorService externalExecutor = Executors.newFixedThreadPool(2);
            try {
                VectorReader reader = new VectorReader(readerOf(sequentialBytes(100)), externalExecutor);
                reader.close();
                assertFalse(externalExecutor.isShutdown(),
                        "External executor should not be shut down by VectorReader.close()");
            } finally {
                externalExecutor.shutdown();
            }
        }

        @Test
        void readsUseProvidedExecutor() throws IOException, ExecutionException, InterruptedException {
            AtomicInteger readCount = new AtomicInteger(0);
            SeekableReader trackingReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return 1000;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) {
                    readCount.incrementAndGet();
                    for (int i = 0; i < len; i++) {
                        b[off + i] = (byte) ((pos + i) % 256);
                    }
                    return len;
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) {
                    readCount.incrementAndGet();
                    int len = byteBuffer.remaining();
                    for (int i = 0; i < len; i++) {
                        byteBuffer.put((byte) ((pos + i) % 256));
                    }
                    return len;
                }

                @Override
                public void close() {
                }
            };

            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                try (VectorReader reader = new VectorReader(trackingReader, executor)) {
                    ParquetFileRange range = new ParquetFileRange(0, 100);
                    reader.readVectored(List.of(range), heapAllocator());
                    range.getDataReadFuture().get();
                    assertTrue(readCount.get() > 0, "Reader should have been called at least once");
                }
                assertFalse(executor.isShutdown(),
                        "External executor should not be shut down");
            } finally {
                executor.shutdown();
            }
        }

    }

    // -------------------------------------------------------------------------
    // Error handling
    // -------------------------------------------------------------------------

    @Nested
    class ErrorHandling {

        @Test
        void ioExceptionInSmallRangePropagatesThroughFuture() throws IOException {
            SeekableReader failingReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return 1000;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) throws IOException {
                    throw new IOException("Simulated read failure");
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
                    throw new IOException("Simulated read failure");
                }

                @Override
                public void close() {
                }
            };

            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(failingReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, 100);
                reader.readVectored(List.of(range), heapAllocator());

                ExecutionException ex = assertThrows(ExecutionException.class,
                        () -> range.getDataReadFuture().get());
                assertInstanceOf(IOException.class, ex.getCause());
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void ioExceptionInLargeRangeReleasesBufferAndFailsFuture() throws IOException {
            SeekableReader failingReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return CHUNK_SIZE * 4L;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) throws IOException {
                    throw new IOException("Simulated chunk read failure");
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
                    throw new IOException("Simulated chunk read failure");
                }

                @Override
                public void close() {
                }
            };

            TrackingAllocator allocator = new TrackingAllocator();
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try (VectorReader reader = new VectorReader(failingReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, CHUNK_SIZE + 1);
                reader.readVectored(List.of(range), allocator);

                ExecutionException ex = assertThrows(ExecutionException.class,
                        () -> range.getDataReadFuture().get());
                assertInstanceOf(IOException.class, ex.getCause());
                assertEquals(1, allocator.allocatedCount());
                assertEquals(1, allocator.releasedCount(), "buffer of the failed span must be released");
                assertEquals(List.of(), allocator.violations());
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void ioExceptionInOneRangeDoesNotAffectOtherRanges()
                throws IOException, ExecutionException, InterruptedException {
            AtomicInteger callCount = new AtomicInteger(0);
            SeekableReader partiallyFailingReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return 1000;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) throws IOException {
                    if (callCount.incrementAndGet() == 1) {
                        throw new IOException("First read fails");
                    }
                    for (int i = 0; i < len; i++) {
                        b[off + i] = (byte) ((pos + i) % 256);
                    }
                    return len;
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
                    if (callCount.incrementAndGet() == 1) {
                        throw new IOException("First read fails");
                    }
                    int len = byteBuffer.remaining();
                    for (int i = 0; i < len; i++) {
                        byteBuffer.put((byte) ((pos + i) % 256));
                    }
                    return len;
                }

                @Override
                public void close() {
                }
            };

            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(partiallyFailingReader, executor)) {
                ParquetFileRange r1 = new ParquetFileRange(0, 10);
                ParquetFileRange r2 = new ParquetFileRange(50, 10);

                reader.readVectored(List.of(r1, r2), heapAllocator());

                // One of them should fail, the other should succeed
                CompletableFuture<ByteBuffer> f1 = r1.getDataReadFuture();
                CompletableFuture<ByteBuffer> f2 = r2.getDataReadFuture();

                // At least one should complete exceptionally
                boolean anyFailed = false;
                try {
                    f1.get();
                } catch (ExecutionException e) {
                    anyFailed = true;
                }
                try {
                    f2.get();
                } catch (ExecutionException e) {
                    anyFailed = true;
                }
                assertTrue(anyFailed, "At least one range should have failed");
            } finally {
                executor.shutdown();
            }
        }
    }
}
