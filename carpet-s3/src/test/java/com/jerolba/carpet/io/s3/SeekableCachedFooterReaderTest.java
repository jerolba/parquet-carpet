package com.jerolba.carpet.io.s3;

import static com.jerolba.carpet.io.s3.SeekableReaderHelper.heapAllocator;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.largeVirtualReader;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.readerOf;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.sequentialBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SeekableCachedFooterReaderTest {

    // The default footer window hard-coded in CachedFooter
    private static final int DEFAULT_FOOTER_SIZE = 8 * 1024 * 1024; // 8 MiB

    // -------------------------------------------------------------------------
    // getPos and seek
    // -------------------------------------------------------------------------

    private SeekableCachedFooterReader readerFor(byte[] data) throws IOException {
        return new SeekableCachedFooterReader(readerOf(data), null);
    }

    @Nested
    class GetPosAndSeek {

        @Test
        void initialPositionIsZero() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                assertEquals(0, reader.getPos());
            }
        }

        @Test
        void seekUpdatesPosition() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.seek(50);
                assertEquals(50, reader.getPos());
            }
        }

        @Test
        void seekBackward() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.seek(80);
                reader.seek(20);
                assertEquals(20, reader.getPos());
            }
        }

        @Test
        void seekToZero() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.seek(50);
                reader.seek(0);
                assertEquals(0, reader.getPos());
            }
        }

        @Test
        void seekToEndOfFile() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.seek(99);
                assertEquals(99, reader.getPos());
            }
        }

        @Test
        void seekToNegativePositionThrowsIOException() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                assertThrows(IOException.class, () -> reader.seek(-1));
            }
        }
    }

    // -------------------------------------------------------------------------
    // read() — single byte
    // -------------------------------------------------------------------------

    @Nested
    class ReadSingleByte {

        @Test
        void readFromCachedPositionReturnsByte() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                reader.seek(5);
                assertEquals(5, reader.read());
            }
        }

        @Test
        void readReturnsUnsignedByte() throws IOException {
            byte[] data = new byte[] { (byte) 0xFF, (byte) 0x80 };
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                assertEquals(255, reader.read()); // position 0
                assertEquals(128, reader.read()); // position 1
            }
        }

        @Test
        void readAdvancesPositionByOne() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.seek(10);
                reader.read();
                assertEquals(11, reader.getPos());
            }
        }

        @Test
        void consecutiveReadsAdvancePosition() throws IOException {
            byte[] data = sequentialBytes(10);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                for (int i = 0; i < 5; i++) {
                    assertEquals(i, reader.read());
                }
                assertEquals(5, reader.getPos());
            }
        }

        @Test
        void readFromNonCachedPositionDelegatesToUnderlyingReader() throws IOException {
            // File larger than 8 MiB: position 0 falls outside the cached footer window.
            // largeVirtualReader: value at position p is (byte)(p % 256).
            long largeSize = (long) DEFAULT_FOOTER_SIZE + 1024;
            try (var reader = new SeekableCachedFooterReader(largeVirtualReader(largeSize), null)) {
                // pos = 0 is not cached → should delegate to seekableReader
                assertEquals(0, reader.read());
                assertEquals(1, reader.getPos());
            }
        }
    }

    // -------------------------------------------------------------------------
    // read(byte[], int, int)
    // -------------------------------------------------------------------------

    @Nested
    class ReadByteArray {

        @Test
        void readFromCachedPositionFillsArray() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                byte[] dest = new byte[10];
                int bytesRead = reader.read(dest, 0, 10);
                assertEquals(10, bytesRead);
                assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, dest);
            }
        }

        @Test
        void readFromCachedPositionWithStartOffset() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                reader.seek(5);
                byte[] dest = new byte[20];
                reader.read(dest, 10, 5);
                assertEquals(5, dest[10]);
                assertEquals(9, dest[14]);
                // Positions outside the written range remain zero
                assertEquals(0, dest[0]);
                assertEquals(0, dest[15]);
            }
        }

        @Test
        void readFromCachedPositionAdvancesPosition() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.seek(20);
                byte[] dest = new byte[10];
                reader.read(dest, 0, 10);
                assertEquals(30, reader.getPos());
            }
        }

        @Test
        void readFromNonCachedPositionDelegatesToUnderlyingReader() throws IOException {
            // largeVirtualReader: value at position p is (byte)(p % 256)
            long largeSize = (long) DEFAULT_FOOTER_SIZE + 1024;
            try (var reader = new SeekableCachedFooterReader(largeVirtualReader(largeSize), null)) {
                // pos = 0 is NOT in the last-8-MiB window
                byte[] dest = new byte[10];
                int bytesRead = reader.read(dest, 0, 10);
                assertEquals(10, bytesRead);
                for (int i = 0; i < 10; i++) {
                    assertEquals((byte) i, dest[i], "Mismatch at index " + i);
                }
            }
        }

        @Test
        void readFromNonCachedPositionAdvancesPosition() throws IOException {
            long largeSize = (long) DEFAULT_FOOTER_SIZE + 1024;
            try (var reader = new SeekableCachedFooterReader(largeVirtualReader(largeSize), null)) {
                byte[] dest = new byte[10];
                reader.read(dest, 0, 10);
                assertEquals(10, reader.getPos());
            }
        }
    }

    // -------------------------------------------------------------------------
    // read(ByteBuffer)
    // -------------------------------------------------------------------------

    @Nested
    class ReadByteBuffer {

        @Test
        void readFromCachedPositionFillsBuffer() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                ByteBuffer buf = ByteBuffer.allocate(10);
                reader.read(buf);
                buf.flip();
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, buf.get() & 0xFF, "Mismatch at index " + i);
                }
            }
        }

        @Test
        void readFromCachedPositionAdvancesPosition() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.seek(10);
                ByteBuffer buf = ByteBuffer.allocate(5);
                reader.read(buf);
                assertEquals(15, reader.getPos());
            }
        }

        @Test
        void readFromCachedPositionReturnsLen() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                ByteBuffer buf = ByteBuffer.allocate(7);
                int result = reader.read(buf);
                assertEquals(7, result);
            }
        }

        @Test
        void readFromNonCachedPositionDelegatesToUnderlyingReader() throws IOException {
            long largeSize = (long) DEFAULT_FOOTER_SIZE + 1024;
            try (var reader = new SeekableCachedFooterReader(largeVirtualReader(largeSize), null)) {
                // pos = 0 is NOT cached
                ByteBuffer buf = ByteBuffer.allocate(5);
                reader.read(buf);
                buf.flip();
                for (int i = 0; i < 5; i++) {
                    assertEquals(i, buf.get() & 0xFF, "Mismatch at index " + i);
                }
            }
        }

        @Test
        void readFromNonCachedPositionAdvancesPosition() throws IOException {
            long largeSize = (long) DEFAULT_FOOTER_SIZE + 1024;
            try (var reader = new SeekableCachedFooterReader(largeVirtualReader(largeSize), null)) {
                ByteBuffer buf = ByteBuffer.allocate(5);
                reader.read(buf);
                assertEquals(5, reader.getPos());
            }
        }

        @Test
        void readIntoDirectByteBuffer() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                reader.seek(5);
                ByteBuffer buf = ByteBuffer.allocateDirect(5);
                reader.read(buf);
                buf.flip();
                assertEquals(5, buf.get(0) & 0xFF);
                assertEquals(9, buf.get(4) & 0xFF);
            }
        }
    }

    // -------------------------------------------------------------------------
    // readFully — delegation
    // -------------------------------------------------------------------------

    @Nested
    class ReadFully {

        @Test
        void readFullyByteArrayReadsFromPosition() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                reader.seek(0);
                byte[] dest = new byte[10];
                reader.readFully(dest);
                assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, dest);
            }
        }

        @Test
        void readFullyByteArrayAdvancesPosition() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.readFully(new byte[15]);
                assertEquals(15, reader.getPos());
            }
        }

        @Test
        void readFullyByteArrayWithStartAndLen() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                reader.seek(5);
                byte[] dest = new byte[10];
                reader.readFully(dest, 2, 5);
                assertEquals(5, dest[2]);
                assertEquals(9, dest[6]);
                // Untouched positions remain zero
                assertEquals(0, dest[0]);
                assertEquals(0, dest[7]);
            }
        }

        @Test
        void readFullyByteBufferReadsFromPosition() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                reader.seek(0);
                ByteBuffer buf = ByteBuffer.allocate(10);
                reader.readFully(buf);
                buf.flip();
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, buf.get() & 0xFF, "Mismatch at index " + i);
                }
            }
        }

        @Test
        void readFullyByteBufferAdvancesPosition() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                reader.readFully(ByteBuffer.allocate(12));
                assertEquals(12, reader.getPos());
            }
        }
    }

    // -------------------------------------------------------------------------
    // readVectored + readVectoredAvailable
    // -------------------------------------------------------------------------

    @Nested
    class ReadVectored {

        @Test
        void readVectoredAvailableAlwaysReturnsTrue() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                assertTrue(reader.readVectoredAvailable(heapAllocator()));
            }
        }

        @Test
        void readVectoredSingleRange() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                ParquetFileRange range = new ParquetFileRange(10, 5);

                reader.readVectored(List.of(range), heapAllocator());

                ByteBuffer result = range.getDataReadFuture().get();
                assertEquals(5, result.remaining());
                assertEquals(10, result.get(0) & 0xFF);
                assertEquals(14, result.get(4) & 0xFF);
            }
        }

        @Test
        void readVectoredMultipleRanges() throws IOException, ExecutionException, InterruptedException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                ParquetFileRange range1 = new ParquetFileRange(0, 3);
                ParquetFileRange range2 = new ParquetFileRange(50, 4);

                reader.readVectored(List.of(range1, range2), heapAllocator());

                ByteBuffer r1 = range1.getDataReadFuture().get();
                assertEquals(3, r1.remaining());
                assertEquals(0, r1.get(0) & 0xFF);
                assertEquals(2, r1.get(2) & 0xFF);

                ByteBuffer r2 = range2.getDataReadFuture().get();
                assertEquals(4, r2.remaining());
                assertEquals(50, r2.get(0) & 0xFF);
                assertEquals(53, r2.get(3) & 0xFF);
            }
        }

        @Test
        void readVectoredFutureIsAlreadyCompleted() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                ParquetFileRange range = new ParquetFileRange(0, 5);

                reader.readVectored(List.of(range), heapAllocator());

                CompletableFuture<ByteBuffer> future = range.getDataReadFuture();
                assertTrue(future.isDone(), "Future should be already completed");
            }
        }

        @Test
        void readVectoredEmptyRangeList() throws IOException {
            try (SeekableCachedFooterReader reader = readerFor(sequentialBytes(100))) {
                assertDoesNotThrow(() -> reader.readVectored(List.of(), heapAllocator()));
            }
        }

        @Test
        void readVectoredPositionAfterLastRange() throws IOException {
            byte[] data = sequentialBytes(100);
            try (SeekableCachedFooterReader reader = readerFor(data)) {
                ParquetFileRange range = new ParquetFileRange(20, 10);

                reader.readVectored(List.of(range), heapAllocator());
            }
        }

        @Test
        void readVectoredParallel() throws IOException, InterruptedException, ExecutionException {
            byte[] data = sequentialBytes(200);
            // Use 4 threads
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try (SeekableCachedFooterReader reader = new SeekableCachedFooterReader(readerOf(data), executor)) {
                ParquetFileRange r1 = new ParquetFileRange(0, 10);
                ParquetFileRange r2 = new ParquetFileRange(20, 10);
                ParquetFileRange r3 = new ParquetFileRange(100, 50);

                reader.readVectored(List.of(r1, r2, r3), heapAllocator());

                CompletableFuture<ByteBuffer> f1 = r1.getDataReadFuture();
                CompletableFuture<ByteBuffer> f2 = r2.getDataReadFuture();
                CompletableFuture<ByteBuffer> f3 = r3.getDataReadFuture();

                // Wait for all to complete
                CompletableFuture.allOf(f1, f2, f3).get();

                assertTrue(f1.isDone());
                assertTrue(f2.isDone());
                assertTrue(f3.isDone());

                ByteBuffer b1 = f1.get();
                ByteBuffer b2 = f2.get();
                ByteBuffer b3 = f3.get();

                assertEquals(10, b1.remaining());
                assertEquals(10, b2.remaining());
                assertEquals(50, b3.remaining());

                byte[] res1 = new byte[10];
                b1.get(res1);
                assertArrayEquals(java.util.Arrays.copyOfRange(data, 0, 10), res1);

                byte[] res2 = new byte[10];
                b2.get(res2);
                assertArrayEquals(java.util.Arrays.copyOfRange(data, 20, 30), res2);

                byte[] res3 = new byte[50];
                b3.get(res3);
                assertArrayEquals(java.util.Arrays.copyOfRange(data, 100, 150), res3);
            } finally {
                executor.shutdown();
            }
        }
    }

    // -------------------------------------------------------------------------
    // close
    // -------------------------------------------------------------------------

    @Nested
    class Close {

        @Test
        void closeDelegatesToUnderlyingReader() throws IOException {
            boolean[] closed = { false };
            SeekableReader trackingReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return 100;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) {
                    return len;
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) {
                    int len = byteBuffer.remaining();
                    byteBuffer.position(byteBuffer.limit());
                    return len;
                }

                @Override
                public void close() {
                    closed[0] = true;
                }
            };

            SeekableCachedFooterReader reader = new SeekableCachedFooterReader(trackingReader, null);
            reader.close();

            assertTrue(closed[0], "Underlying SeekableReader should have been closed");
        }

        @Test
        void closeIsIdempotent() throws IOException {
            int[] closeCount = { 0 };
            SeekableReader trackingReader = new SeekableReader() {
                @Override
                public long getLength() {
                    return 100;
                }

                @Override
                public int readFully(long pos, byte[] b, int off, int len) {
                    return len;
                }

                @Override
                public int readFully(long pos, ByteBuffer byteBuffer) {
                    byteBuffer.position(byteBuffer.limit());
                    return byteBuffer.limit() - byteBuffer.position();
                }

                @Override
                public void close() {
                    closeCount[0]++;
                }
            };

            SeekableCachedFooterReader reader = new SeekableCachedFooterReader(trackingReader, null);
            reader.close();
            reader.close(); // second close must be a no-op

            assertEquals(1, closeCount[0], "Underlying reader should be closed exactly once");
        }

    }
}
