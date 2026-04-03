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

import static com.jerolba.carpet.io.s3.SeekableReaderHelper.heapAllocator;
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

import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SequentialReaderTest {

    // -------------------------------------------------------------------------
    // Read single range
    // -------------------------------------------------------------------------

    @Nested
    class ReadSingleRange {

        @Test
        void readRangeAtOffsetZero() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(100);
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
                ParquetFileRange range = new ParquetFileRange(0, 10);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(10, result.remaining());
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, result.get(i) & 0xFF, "Mismatch at index " + i);
                }
            }
        }

        @Test
        void readRangeAtNonZeroOffset() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(200);
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
                ParquetFileRange range = new ParquetFileRange(50, 20);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(20, result.remaining());
                for (int i = 0; i < 20; i++) {
                    assertEquals((50 + i) % 256, result.get(i) & 0xFF, "Mismatch at index " + i);
                }
            }
        }

        @Test
        void readRangeReturnsFutureAlreadyCompleted() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
                ParquetFileRange range = new ParquetFileRange(0, 10);
                reader.readVectored(List.of(range), heapAllocator());

                CompletableFuture<ByteBuffer> future = range.getDataReadFuture();
                assertTrue(future.isDone(), "Future should be immediately completed");
                assertFalse(future.isCompletedExceptionally());
            }
        }

        @Test
        void readRangeBufferIsFlipped() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(100);
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
                ParquetFileRange range = new ParquetFileRange(10, 30);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(0, result.position());
                assertEquals(30, result.limit());
                assertEquals(30, result.remaining());
            }
        }

        @Test
        void readEntireContent() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(256);
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
                ParquetFileRange range = new ParquetFileRange(0, 256);
                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                byte[] actual = new byte[256];
                result.get(actual);
                assertArrayEquals(data, actual);
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
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
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
            }
        }

        @Test
        void readAdjacentRanges() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(100);
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
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
            }
        }

        @Test
        void eachRangeFutureIsImmediatelyCompleted() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
                ParquetFileRange r1 = new ParquetFileRange(0, 10);
                ParquetFileRange r2 = new ParquetFileRange(50, 10);

                reader.readVectored(List.of(r1, r2), heapAllocator());

                assertTrue(r1.getDataReadFuture().isDone());
                assertTrue(r2.getDataReadFuture().isDone());
            }
        }

        @Test
        void readEmptyRangeList() throws IOException {
            try (SequentialReader reader = new SequentialReader(readerOf(sequentialBytes(100)))) {
                assertDoesNotThrow(() -> reader.readVectored(List.of(), heapAllocator()));
            }
        }

        @Test
        void rangesAreReadInOrder() throws IOException, ExecutionException, InterruptedException {
            // SequentialReader processes ranges serially; verify each range sees the
            // correct data regardless of order in the list
            byte[] data = sequentialBytes(200);
            try (SequentialReader reader = new SequentialReader(readerOf(data))) {
                ParquetFileRange r1 = new ParquetFileRange(100, 10);
                ParquetFileRange r2 = new ParquetFileRange(0, 10);

                reader.readVectored(List.of(r1, r2), heapAllocator());

                byte[] res1 = new byte[10];
                r1.getDataReadFuture().get().get(res1);
                assertArrayEquals(Arrays.copyOfRange(data, 100, 110), res1);

                byte[] res2 = new byte[10];
                r2.getDataReadFuture().get().get(res2);
                assertArrayEquals(Arrays.copyOfRange(data, 0, 10), res2);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Error handling
    // -------------------------------------------------------------------------

    @Nested
    class ErrorHandling {

        @Test
        void ioExceptionInFirstRangePropagatesFromReadVectored() throws IOException {
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

            try (SequentialReader reader = new SequentialReader(failingReader)) {
                ParquetFileRange range = new ParquetFileRange(0, 100);
                // SequentialReader is synchronous: IOException thrown directly from readVectored
                assertThrows(IOException.class,
                        () -> reader.readVectored(List.of(range), heapAllocator()));
            }
        }

        @Test
        void ioExceptionOnSecondRangeLeavesFirstCompleted()
                throws IOException, ExecutionException, InterruptedException {
            // Ranges must be far enough apart (> RANGE_GAP = 1 MB) to remain in
            // separate groups, so that the first read can succeed before the second fails.
            int gap = 2 * 1024 * 1024; // 2 MB > RANGE_GAP
            byte[] data = sequentialBytes(gap + 20);
            int[] callCount = { 0 };
            SeekableReader partiallyFailingReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return data.length;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) throws IOException {
                    if (++callCount[0] > 1) {
                        throw new IOException("Second read fails");
                    }
                    System.arraycopy(data, (int) pos, b, off, len);
                    return len;
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
                    if (++callCount[0] > 1) {
                        throw new IOException("Second read fails");
                    }
                    int len = byteBuffer.remaining();
                    byteBuffer.put(data, (int) pos, len);
                    return len;
                }

                @Override
                public void close() {
                }
            };

            try (SequentialReader reader = new SequentialReader(partiallyFailingReader)) {
                ParquetFileRange r1 = new ParquetFileRange(0, 10);
                ParquetFileRange r2 = new ParquetFileRange(gap, 10);

                assertThrows(IOException.class,
                        () -> reader.readVectored(List.of(r1, r2), heapAllocator()));

                // First range was read and set before the second group failed
                CompletableFuture<ByteBuffer> f1 = r1.getDataReadFuture();
                assertTrue(f1.isDone());
                byte[] res = new byte[10];
                f1.get().get(res);
                assertArrayEquals(Arrays.copyOfRange(data, 0, 10), res);
            }
        }

        @Test
        void ioExceptionBubblesUpAsIOException() {
            SeekableReader failingReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return 1000;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) throws IOException {
                    throw new IOException("underlying failure");
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
                    throw new IOException("underlying failure");
                }

                @Override
                public void close() {
                }
            };

            try (SequentialReader reader = new SequentialReader(failingReader)) {
                IOException ex = assertThrows(IOException.class,
                        () -> reader.readVectored(List.of(new ParquetFileRange(0, 10)), heapAllocator()));
                assertInstanceOf(IOException.class, ex);
                assertEquals("underlying failure", ex.getMessage());
            }
        }
    }
}
