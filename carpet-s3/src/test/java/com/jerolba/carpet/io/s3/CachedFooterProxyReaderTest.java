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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CachedFooterProxyReaderTest {

    /**
     * Mock-like in-memory SeekableReader backed by a byte array that tracks calls.
     */
    static class VerificationReader implements SeekableReader {

        private final byte[] data;
        int readCalls = 0;
        int closeCalls = 0;

        VerificationReader(byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public int readFully(long pos, byte[] b, int off, int len) {
            readCalls++;
            System.arraycopy(data, (int) pos, b, off, len);
            return len;
        }

        @Override
        public int readFully(long pos, ByteBuffer byteBuffer) {
            readCalls++;
            int len = byteBuffer.remaining();
            byteBuffer.put(data, (int) pos, len);
            return len;
        }

        @Override
        public void close() {
            closeCalls++;
        }

    }

    static SeekableReader readerOf(byte[] data) {
        return new VerificationReader(data);
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
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertTrue(footer.isCached(0));
                assertTrue(footer.isCached(99));
            }
        }

        @Test
        void whenContentExactlyFooterSize_entireContentIsCached() throws IOException {
            byte[] data = sequentialBytes(1024);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertTrue(footer.isCached(0));
                assertTrue(footer.isCached(1023));
            }
        }

        @Test
        void whenContentLargerThanFooterSize_onlyTailIsCached() throws IOException {
            byte[] data = sequentialBytes(2000);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertFalse(footer.isCached(0));
                assertFalse(footer.isCached(975)); // just before the footer window
                assertTrue(footer.isCached(976)); // footerPosition = 2000 - 1024 = 976
                assertTrue(footer.isCached(1999));
            }
        }

        @Test
        void defaultConstructorUses8MiBFooterSize() throws IOException {
            // Content smaller than 8 MiB → everything cached from position 0
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data))) {
                assertTrue(footer.isCached(0));
            }
        }

        @Test
        void systemPropertyOverridesDefaultFooterSize() throws IOException {
            System.setProperty(CachedFooterProxyReader.CARPET_FOOTER_CACHE_SIZE, "512");
            try {
                byte[] data = sequentialBytes(1000);
                // footerPosition = 1000 - 512 = 488
                try (var footer = new CachedFooterProxyReader(readerOf(data))) {
                    assertFalse(footer.isCached(487));
                    assertTrue(footer.isCached(488));
                }
            } finally {
                System.clearProperty(CachedFooterProxyReader.CARPET_FOOTER_CACHE_SIZE);
            }
        }
    }

    // -------------------------------------------------------------------------
    // isCached
    // -------------------------------------------------------------------------

    @Nested
    class IsCachedTests {

        @Test
        void negativePositionIsNotCached() throws IOException {
            try (var footer = new CachedFooterProxyReader(readerOf(sequentialBytes(100)), 1024)) {
                assertFalse(footer.isCached(-1));
            }
        }

        @Test
        void positionBeyondContentLengthIsNotCached() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertFalse(footer.isCached(101));
                assertFalse(footer.isCached(Long.MAX_VALUE));
            }
        }

        @Test
        void firstAndLastValidPositionsAreCached() throws IOException {
            byte[] data = sequentialBytes(500);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertTrue(footer.isCached(0));
                assertTrue(footer.isCached(499)); // last valid byte
            }
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
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertEquals(0, footer.read(0));
                assertEquals(1, footer.read(1));
                assertEquals(99, footer.read(99));
            }
        }

        @Test
        void readReturnsUnsignedByte() throws IOException {
            byte[] data = new byte[] { (byte) 0xFF, (byte) 0x80 };
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertEquals(255, footer.read(0));
                assertEquals(128, footer.read(1));
            }
        }

        @Test
        void readFromNonCachedPositionThrows() throws IOException {
            byte[] data = sequentialBytes(2000);
            // footerPosition = 2000 - 1024 = 976; position 0 is not cached
            assertThrows(UnsupportedOperationException.class, () -> {
                try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                    footer.read(0);
                }
            });
        }

        @Test
        void readFromNegativePositionThrows() throws IOException {
            assertThrows(UnsupportedOperationException.class, () -> {
                try (var footer = new CachedFooterProxyReader(readerOf(sequentialBytes(100)),
                        1024)) {
                    footer.read(-1);
                }
            });
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
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                byte[] dest = new byte[10];
                footer.readBytesFromPos(0, dest, 0, 10);

                assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, dest);
            }
        }

        @Test
        void readBytesWithOffset() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                byte[] dest = new byte[20];
                footer.readBytesFromPos(5, dest, 10, 5);

                assertEquals(5, dest[10]);
                assertEquals(9, dest[14]);
                // Positions outside the written range remain zero
                assertEquals(0, dest[0]);
                assertEquals(0, dest[15]);
            }
        }

        @Test
        void readAllBytesFromFooterWindow() throws IOException {
            byte[] data = sequentialBytes(2000);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                byte[] dest = new byte[1024];
                footer.readBytesFromPos(976, dest, 0, 1024); // footerPosition = 976

                for (int i = 0; i < 1024; i++) {
                    assertEquals(data[976 + i], dest[i], "Mismatch at index " + i);
                }
            }
        }

        @Test
        void readMoreBytesThanAvailableInFooterThrows() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                byte[] dest = new byte[200];
                // Only 10 bytes remain from position 90 to end (99 inclusive)
                assertThrows(IndexOutOfBoundsException.class,
                        () -> footer.readBytesFromPos(90, dest, 0, 20));
            }
        }

        @Test
        void readFromNonCachedPositionThrows() throws IOException {
            byte[] data = sequentialBytes(2000);
            byte[] dest = new byte[10];
            assertThrows(UnsupportedOperationException.class,
                    () -> {
                        try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                            footer.readBytesFromPos(0, dest, 0, 10);
                        }
                    });
        }

        @Test
        void readWithStartPlusLenExceedingDestArrayThrows() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                byte[] dest = new byte[10];
                // start=8, len=5 → writes to dest[8..12] which is out of bounds
                assertThrows(IndexOutOfBoundsException.class,
                        () -> footer.readBytesFromPos(0, dest, 8, 5));
            }
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
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                ByteBuffer buf = ByteBuffer.allocate(10);
                footer.readBytesFromPos(0, buf);

                buf.flip();
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, buf.get() & 0xFF);
                }
            }
        }

        @Test
        void readBytesIntoByteBufferFromOffset() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                ByteBuffer buf = ByteBuffer.allocate(5);
                footer.readBytesFromPos(10, buf);

                buf.flip();
                assertEquals(10, buf.get() & 0xFF);
                assertEquals(14, buf.get(4) & 0xFF);
            }
        }

        @Test
        void readMoreBytesThanAvailableInBufferThrows() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                // Only 5 bytes remain from position 95, but buffer requests 10
                ByteBuffer buf = ByteBuffer.allocate(10);
                assertThrows(IndexOutOfBoundsException.class,
                        () -> footer.readBytesFromPos(95, buf));
            }
        }

        @Test
        void readFromNonCachedPositionThrows() throws IOException {
            byte[] data = sequentialBytes(2000);
            ByteBuffer buf = ByteBuffer.allocate(10);
            assertThrows(UnsupportedOperationException.class,
                    () -> {
                        try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                            footer.readBytesFromPos(0, buf);
                        }
                    });
        }

        @Test
        void readIntoDirectByteBuffer() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                ByteBuffer buf = ByteBuffer.allocateDirect(10);
                footer.readBytesFromPos(5, buf);

                buf.flip();
                assertEquals(5, buf.get() & 0xFF);
                assertEquals(14, buf.get(9) & 0xFF);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Delegation / Public methods
    // -------------------------------------------------------------------------

    @Nested
    class DelegationTests {

        @Test
        void getLengthDelegatesToReader() throws IOException {
            byte[] data = sequentialBytes(100);
            VerificationReader reader = new VerificationReader(data);
            try (var footer = new CachedFooterProxyReader(reader)) {
                assertEquals(100, footer.getLength());
            }
        }

        @Test
        void readFullyArray_WhenCached_DoesNotCallReader() throws IOException {
            byte[] data = sequentialBytes(2000);
            VerificationReader reader = new VerificationReader(data);
            // Footer size 1024, footerPosition = 2000 - 1024 = 976
            try (var footer = new CachedFooterProxyReader(reader, 1024)) {
                int initialCalls = reader.readCalls; // Should be 1 from constructor cache warming

                byte[] dest = new byte[10];
                int read = footer.readFully(1000, dest, 0, 10);

                assertEquals(10, read);
                assertEquals(initialCalls, reader.readCalls, "Should not have called reader for cached data");
                assertEquals(data[1000], dest[0]);
            }
        }

        @Test
        void readFullyArray_WhenNotCached_DelegatesToReader() throws IOException {
            byte[] data = sequentialBytes(2000);
            VerificationReader reader = new VerificationReader(data);
            try (var footer = new CachedFooterProxyReader(reader, 1024)) {
                int initialCalls = reader.readCalls;

                byte[] dest = new byte[10];
                int read = footer.readFully(0, dest, 0, 10);

                assertEquals(10, read);
                assertEquals(initialCalls + 1, reader.readCalls, "Should have called reader for non-cached data");
                assertEquals(data[0], dest[0]);
            }
        }

        @Test
        void readFullyBuffer_WhenCached_DoesNotCallReader() throws IOException {
            byte[] data = sequentialBytes(2000);
            VerificationReader reader = new VerificationReader(data);
            try (var footer = new CachedFooterProxyReader(reader, 1024)) {
                int initialCalls = reader.readCalls;

                ByteBuffer buf = ByteBuffer.allocate(10);
                int read = footer.readFully(1000, buf);

                assertEquals(10, read);
                assertEquals(initialCalls, reader.readCalls, "Should not have called reader for cached data");
                buf.flip();
                assertEquals(data[1000], buf.get());
            }
        }

        @Test
        void readFullyBuffer_WhenNotCached_DelegatesToReader() throws IOException {
            byte[] data = sequentialBytes(2000);
            VerificationReader reader = new VerificationReader(data);
            try (var footer = new CachedFooterProxyReader(reader, 1024)) {
                int initialCalls = reader.readCalls;

                ByteBuffer buf = ByteBuffer.allocate(10);
                int read = footer.readFully(0, buf);

                assertEquals(10, read);
                assertEquals(initialCalls + 1, reader.readCalls, "Should have called reader for non-cached data");
                buf.flip();
                assertEquals(data[0], buf.get());
            }
        }

        @Test
        void closeClosesInjectedReader() throws IOException {
            VerificationReader reader = new VerificationReader(sequentialBytes(2000));
            var footer = new CachedFooterProxyReader(reader);
            footer.close();
            assertEquals(1, reader.closeCalls, "Injected reader should be closed");
        }
    }
}
