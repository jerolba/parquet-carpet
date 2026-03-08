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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class VectorReaderTest {

    private static final int CHUNK_SIZE = 8 * 1024 * 1024; // 8 MB

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
    // Read large range in parallel chunks (> CHUNK_SIZE)
    // -------------------------------------------------------------------------

    @Nested
    class ReadLargeRangeInChunks {

        @Test
        void readRangeSlightlyAboveChunkSize() throws IOException, ExecutionException, InterruptedException {
            int rangeLength = CHUNK_SIZE + 1;
            SeekableReader virtualReader = largeVirtualReader(rangeLength * 2L);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, rangeLength);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(rangeLength, result.remaining());
                assertEquals(0, result.position());

                // Verify boundary bytes
                assertEquals(0, result.get(0) & 0xFF);
                assertEquals((CHUNK_SIZE - 1) % 256, result.get(CHUNK_SIZE - 1) & 0xFF);
                assertEquals(CHUNK_SIZE % 256, result.get(CHUNK_SIZE) & 0xFF);
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void readRangeSpanningThreeFullChunks() throws IOException, ExecutionException, InterruptedException {
            int rangeLength = CHUNK_SIZE * 3;
            SeekableReader virtualReader = largeVirtualReader(rangeLength * 2L);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, rangeLength);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(rangeLength, result.remaining());
                assertEquals(0, result.position());

                // Verify first byte of each chunk
                assertEquals(0, result.get(0) & 0xFF);
                assertEquals(0, result.get(CHUNK_SIZE) & 0xFF); // CHUNK_SIZE % 256 == 0
                assertEquals(0, result.get(CHUNK_SIZE * 2) & 0xFF);

                // Verify last byte of each chunk
                assertEquals((CHUNK_SIZE - 1) % 256, result.get(CHUNK_SIZE - 1) & 0xFF);
                assertEquals((CHUNK_SIZE * 2 - 1) % 256, result.get(CHUNK_SIZE * 2 - 1) & 0xFF);
                assertEquals((CHUNK_SIZE * 3 - 1) % 256, result.get(CHUNK_SIZE * 3 - 1) & 0xFF);
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void readRangeNotMultipleOfChunkSize() throws IOException, ExecutionException, InterruptedException {
            // 2.5 chunks: 2 full + 1 partial (half chunk)
            int rangeLength = CHUNK_SIZE * 2 + CHUNK_SIZE / 2;
            SeekableReader virtualReader = largeVirtualReader(rangeLength * 2L);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, rangeLength);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(rangeLength, result.remaining());

                // Verify last byte of partial chunk
                assertEquals((rangeLength - 1) % 256, result.get(rangeLength - 1) & 0xFF);

                // Verify bytes around chunk boundaries
                assertEquals((CHUNK_SIZE - 1) % 256, result.get(CHUNK_SIZE - 1) & 0xFF);
                assertEquals(CHUNK_SIZE % 256, result.get(CHUNK_SIZE) & 0xFF);
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void readLargeRangeAtNonZeroOffset() throws IOException, ExecutionException, InterruptedException {
            int rangeLength = CHUNK_SIZE + 100;
            long offset = 1000;
            SeekableReader virtualReader = largeVirtualReader(offset + rangeLength + 1000);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(offset, rangeLength);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(rangeLength, result.remaining());

                // Verify first bytes match expected values at offset 1000
                for (int i = 0; i < 100; i++) {
                    assertEquals((int) ((offset + i) % 256), result.get(i) & 0xFF,
                            "Mismatch at buffer index " + i);
                }
                // Verify bytes in the second chunk (past CHUNK_SIZE boundary)
                for (int i = CHUNK_SIZE; i < CHUNK_SIZE + 100; i++) {
                    assertEquals((int) ((offset + i) % 256), result.get(i) & 0xFF,
                            "Mismatch at buffer index " + i);
                }
            } finally {
                executor.shutdown();
            }
        }

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
        void largeRangeAllBytesCorrect() throws IOException, ExecutionException, InterruptedException {
            // Verify every byte in a multi-chunk range
            int rangeLength = CHUNK_SIZE + 4096;
            SeekableReader virtualReader = largeVirtualReader(rangeLength * 2L);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, rangeLength);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(0, result.position());
                assertEquals(rangeLength, result.limit());

                byte[] actual = new byte[rangeLength];
                result.get(actual);
                for (int i = 0; i < rangeLength; i++) {
                    assertEquals((byte) (i % 256), actual[i], "Mismatch at index " + i);
                }
            } finally {
                executor.shutdown();
            }
        }

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

        @Test
        void largeRangeAtOffsetAllBytesCorrect() throws IOException, ExecutionException, InterruptedException {
            int rangeLength = CHUNK_SIZE * 2 + 1000;
            long offset = 500;
            SeekableReader virtualReader = largeVirtualReader(offset + rangeLength);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try (VectorReader reader = new VectorReader(virtualReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(offset, rangeLength);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                byte[] actual = new byte[rangeLength];
                result.get(actual);

                // Sample positions across all chunks
                for (int i = 0; i < rangeLength; i += 1024) {
                    assertEquals((byte) ((offset + i) % 256), actual[i],
                            "Mismatch at index " + i);
                }
                // Also verify last byte
                assertEquals((byte) ((offset + rangeLength - 1) % 256), actual[rangeLength - 1]);
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
        void closeShutdownsOwnedExecutor() throws IOException {
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                VectorReader reader = new VectorReader(readerOf(sequentialBytes(10)), executor);
                reader.close();
            } finally {
                executor.shutdown();
            }
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
        void ioExceptionInLargeRangePropagatesThroughFuture() throws IOException {
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

            ExecutorService executor = Executors.newFixedThreadPool(4);
            try (VectorReader reader = new VectorReader(failingReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, CHUNK_SIZE + 1);
                reader.readVectored(List.of(range), heapAllocator());

                ExecutionException ex = assertThrows(ExecutionException.class,
                        () -> range.getDataReadFuture().get());
                // The exception chain: ExecutionException -> RuntimeException -> IOException
                Throwable cause = ex.getCause();
                // CompletableFuture.allOf may wrap in CompletionException
                assertTrue(
                        cause instanceof RuntimeException || cause instanceof java.util.concurrent.CompletionException,
                        "Expected RuntimeException or CompletionException but got " + cause.getClass());
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

    // -------------------------------------------------------------------------
    // Concurrency verification
    // -------------------------------------------------------------------------

    @Nested
    class ConcurrencyVerification {

        @Test
        void largeRangeIsReadInParallelChunks() throws IOException, ExecutionException, InterruptedException {
            // Track which threads perform reads to verify parallelism
            AtomicInteger concurrentReads = new AtomicInteger(0);
            AtomicInteger maxConcurrency = new AtomicInteger(0);

            SeekableReader slowReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return CHUNK_SIZE * 4L;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) throws IOException {
                    int current = concurrentReads.incrementAndGet();
                    maxConcurrency.updateAndGet(max -> Math.max(max, current));
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    for (int i = 0; i < len; i++) {
                        b[off + i] = (byte) ((pos + i) % 256);
                    }
                    concurrentReads.decrementAndGet();
                    return len;
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
                    int current = concurrentReads.incrementAndGet();
                    maxConcurrency.updateAndGet(max -> Math.max(max, current));
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    int len = byteBuffer.remaining();
                    for (int i = 0; i < len; i++) {
                        byteBuffer.put((byte) ((pos + i) % 256));
                    }
                    concurrentReads.decrementAndGet();
                    return len;
                }

                @Override
                public void close() {
                }
            };

            // 3 chunks means 3 parallel tasks with 4 threads available
            int rangeLength = CHUNK_SIZE * 3;
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try (VectorReader reader = new VectorReader(slowReader, executor)) {
                ParquetFileRange range = new ParquetFileRange(0, rangeLength);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(rangeLength, result.remaining());

                // With 4 threads and 3 chunks, we expect at least 2 concurrent reads
                assertTrue(maxConcurrency.get() >= 2,
                        "Expected parallel execution but max concurrency was " + maxConcurrency.get());
            }
        }
    }
}
