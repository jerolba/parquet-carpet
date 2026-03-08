package com.jerolba.carpet.io.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CachedFooterTest {

    /**
     * Simple in-memory SeekableReader backed by a byte array.
     */
    static SeekableReader readerOf(byte[] data) {
        return new SeekableReader() {
            @Override
            public long getLength() {
                return data.length;
            }

            @Override
            public int readFully(long pos, byte[] b, int off, int len) {
                System.arraycopy(data, (int) pos, b, off, len);
                return len;
            }

            @Override
            public int readFully(long pos, ByteBuffer byteBuffer) {
                int len = byteBuffer.remaining();
                byteBuffer.put(data, (int) pos, len);
                return len;
            }

            @Override
            public void close() {
            }
        };
    }

    static byte[] sequentialBytes(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    @Nested
    class ConstructorTests {

        @Test
        void whenContentSmallerThanFooterSize_entireContentIsCached() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            assertTrue(footer.isCached(0));
            assertTrue(footer.isCached(99));
        }

        @Test
        void whenContentExactlyFooterSize_entireContentIsCached() throws IOException {
            byte[] data = sequentialBytes(1024);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            assertTrue(footer.isCached(0));
            assertTrue(footer.isCached(1023));
        }

        @Test
        void whenContentLargerThanFooterSize_onlyTailIsCached() throws IOException {
            byte[] data = sequentialBytes(2000);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            assertFalse(footer.isCached(0));
            assertFalse(footer.isCached(975)); // just before the footer window
            assertTrue(footer.isCached(976)); // footerPosition = 2000 - 1024 = 976
            assertTrue(footer.isCached(1999));
        }

        @Test
        void defaultConstructorUses8MiBFooterSize() throws IOException {
            // Content smaller than 8 MiB → everything cached from position 0
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data));

            assertTrue(footer.isCached(0));
        }
    }

    // -------------------------------------------------------------------------
    // isCached
    // -------------------------------------------------------------------------

    @Nested
    class IsCachedTests {

        @Test
        void negativePositionIsNotCached() throws IOException {
            CachedFooter footer = new CachedFooter(readerOf(sequentialBytes(100)), 1024);
            assertFalse(footer.isCached(-1));
        }

        @Test
        void positionBeyondContentLengthIsNotCached() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            assertFalse(footer.isCached(101));
            assertFalse(footer.isCached(Long.MAX_VALUE));
        }

        @Test
        void firstAndLastValidPositionsAreCached() throws IOException {
            byte[] data = sequentialBytes(500);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            assertTrue(footer.isCached(0));
            assertTrue(footer.isCached(499)); // last valid byte
        }
    }

    // -------------------------------------------------------------------------
    // read(long position)
    // -------------------------------------------------------------------------

    @Nested
    class ReadByteTests {

        @Test
        void readSingleByteFromCachedPosition() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            assertEquals(0, footer.read(0));
            assertEquals(1, footer.read(1));
            assertEquals(99, footer.read(99));
        }

        @Test
        void readReturnsUnsignedByte() throws IOException {
            byte[] data = new byte[] { (byte) 0xFF, (byte) 0x80 };
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            assertEquals(255, footer.read(0));
            assertEquals(128, footer.read(1));
        }

        @Test
        void readFromNonCachedPositionThrows() throws IOException {
            byte[] data = sequentialBytes(2000);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            // footerPosition = 2000 - 1024 = 976; position 0 is not cached
            assertThrows(UnsupportedOperationException.class, () -> footer.read(0));
        }

        @Test
        void readFromNegativePositionThrows() throws IOException {
            CachedFooter footer = new CachedFooter(readerOf(sequentialBytes(100)), 1024);
            assertThrows(UnsupportedOperationException.class, () -> footer.read(-1));
        }
    }

    // -------------------------------------------------------------------------
    // readBytesFromPos(long pos, byte[], int, int)
    // -------------------------------------------------------------------------

    @Nested
    class ReadBytesArrayTests {

        @Test
        void readBytesFromBeginningOfContent() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            byte[] dest = new byte[10];
            footer.readBytesFromPos(0, dest, 0, 10);

            assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, dest);
        }

        @Test
        void readBytesWithOffset() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            byte[] dest = new byte[20];
            footer.readBytesFromPos(5, dest, 10, 5);

            assertEquals(5, dest[10]);
            assertEquals(9, dest[14]);
            // Positions outside the written range remain zero
            assertEquals(0, dest[0]);
            assertEquals(0, dest[15]);
        }

        @Test
        void readAllBytesFromFooterWindow() throws IOException {
            byte[] data = sequentialBytes(2000);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            byte[] dest = new byte[1024];
            footer.readBytesFromPos(976, dest, 0, 1024); // footerPosition = 976

            for (int i = 0; i < 1024; i++) {
                assertEquals(data[976 + i], dest[i], "Mismatch at index " + i);
            }
        }

        @Test
        void readMoreBytesThanAvailableInFooterThrows() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            byte[] dest = new byte[200];
            // Only 10 bytes remain from position 90 to end (99 inclusive)
            assertThrows(IndexOutOfBoundsException.class,
                    () -> footer.readBytesFromPos(90, dest, 0, 20));
        }

        @Test
        void readFromNonCachedPositionThrows() throws IOException {
            byte[] data = sequentialBytes(2000);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            byte[] dest = new byte[10];
            assertThrows(UnsupportedOperationException.class,
                    () -> footer.readBytesFromPos(0, dest, 0, 10));
        }

        @Test
        void readWithStartPlusLenExceedingDestArrayThrows() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            byte[] dest = new byte[10];
            // start=8, len=5 → writes to dest[8..12] which is out of bounds
            assertThrows(IndexOutOfBoundsException.class,
                    () -> footer.readBytesFromPos(0, dest, 8, 5));
        }
    }

    // -------------------------------------------------------------------------
    // readBytesFromPos(long pos, ByteBuffer)
    // -------------------------------------------------------------------------

    @Nested
    class ReadBytesBufferTests {

        @Test
        void readBytesIntoByteBuffer() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            ByteBuffer buf = ByteBuffer.allocate(10);
            footer.readBytesFromPos(0, buf);

            buf.flip();
            for (int i = 0; i < 10; i++) {
                assertEquals(i, buf.get() & 0xFF);
            }
        }

        @Test
        void readBytesIntoByteBufferFromOffset() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            ByteBuffer buf = ByteBuffer.allocate(5);
            footer.readBytesFromPos(10, buf);

            buf.flip();
            assertEquals(10, buf.get() & 0xFF);
            assertEquals(14, buf.get(4) & 0xFF);
        }

        @Test
        void readMoreBytesThanAvailableInBufferThrows() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            // Only 5 bytes remain from position 95, but buffer requests 10
            ByteBuffer buf = ByteBuffer.allocate(10);
            assertThrows(IndexOutOfBoundsException.class,
                    () -> footer.readBytesFromPos(95, buf));
        }

        @Test
        void readFromNonCachedPositionThrows() throws IOException {
            byte[] data = sequentialBytes(2000);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            ByteBuffer buf = ByteBuffer.allocate(10);
            assertThrows(UnsupportedOperationException.class,
                    () -> footer.readBytesFromPos(0, buf));
        }

        @Test
        void readIntoDirectByteBuffer() throws IOException {
            byte[] data = sequentialBytes(100);
            CachedFooter footer = new CachedFooter(readerOf(data), 1024);

            ByteBuffer buf = ByteBuffer.allocateDirect(10);
            footer.readBytesFromPos(5, buf);

            buf.flip();
            assertEquals(5, buf.get() & 0xFF);
            assertEquals(14, buf.get(9) & 0xFF);
        }
    }
}
