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

import static com.jerolba.carpet.io.s3.SeekableReaderHelper.readerOf;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.sequentialBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.io.s3.PrefetchTestSupport.RecordingReader;

class CachedFooterProxyReaderTest {

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    @Nested
    class ConstructorTests {

        @Test
        void contentSmallerThanFooterSizeIsCachedEntirely() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertTrue(footer.isCached(0));
                assertTrue(footer.isCached(99));
            }
        }

        @Test
        void contentExactlyFooterSizeIsCachedEntirely() throws IOException {
            byte[] data = sequentialBytes(1024);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                assertTrue(footer.isCached(0));
                assertTrue(footer.isCached(1023));
            }
        }

        @Test
        void contentLargerThanFooterSizeCachesOnlyTheTail() throws IOException {
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
            System.setProperty(S3InputFile.CARPET_FOOTER_CACHE_SIZE, "512");
            try {
                byte[] data = sequentialBytes(1000);
                // footerPosition = 1000 - 512 = 488
                try (var footer = new CachedFooterProxyReader(readerOf(data))) {
                    assertFalse(footer.isCached(487));
                    assertTrue(footer.isCached(488));
                }
            } finally {
                System.clearProperty(S3InputFile.CARPET_FOOTER_CACHE_SIZE);
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
    // readFully(long pos, byte[], int, int) served from the cache
    // -------------------------------------------------------------------------

    @Nested
    class ReadFullyArrayFromCache {

        @Test
        void readBytesFromBeginningOfContent() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                byte[] dest = new byte[10];
                footer.readFully(0, dest, 0, 10);

                assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, dest);
            }
        }

        @Test
        void readBytesWithOffset() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                byte[] dest = new byte[20];
                footer.readFully(5, dest, 10, 5);

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
                footer.readFully(976, dest, 0, 1024); // footerPosition = 976

                for (int i = 0; i < 1024; i++) {
                    assertEquals(data[976 + i], dest[i], "Mismatch at index " + i);
                }
            }
        }

        @Test
        void readWithStartPlusLenExceedingDestArrayThrows() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                byte[] dest = new byte[10];
                // start=8, len=5 → writes to dest[8..12] which is out of bounds
                assertThrows(IndexOutOfBoundsException.class,
                        () -> footer.readFully(0, dest, 8, 5));
            }
        }
    }

    // -------------------------------------------------------------------------
    // readFully(long pos, ByteBuffer) served from the cache
    // -------------------------------------------------------------------------

    @Nested
    class ReadFullyBufferFromCache {

        @Test
        void readBytesIntoByteBuffer() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                ByteBuffer buf = ByteBuffer.allocate(10);
                footer.readFully(0, buf);

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
                footer.readFully(10, buf);

                buf.flip();
                assertEquals(10, buf.get() & 0xFF);
                assertEquals(14, buf.get(4) & 0xFF);
            }
        }

        @Test
        void readIntoDirectByteBuffer() throws IOException {
            byte[] data = sequentialBytes(100);
            try (var footer = new CachedFooterProxyReader(readerOf(data), 1024)) {
                ByteBuffer buf = ByteBuffer.allocateDirect(10);
                footer.readFully(5, buf);

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
            RecordingReader reader = new RecordingReader(readerOf(data));
            try (var footer = new CachedFooterProxyReader(reader)) {
                assertEquals(100, footer.getLength());
            }
        }

        @Test
        void readFullyArrayFromCachedRangeDoesNotCallReader() throws IOException {
            byte[] data = sequentialBytes(2000);
            RecordingReader reader = new RecordingReader(readerOf(data));
            // Footer size 1024, footerPosition = 2000 - 1024 = 976
            try (var footer = new CachedFooterProxyReader(reader, 1024)) {
                int initialCalls = reader.reads().size(); // 1, the constructor filling the cache

                byte[] dest = new byte[10];
                int read = footer.readFully(1000, dest, 0, 10);

                assertEquals(10, read);
                assertEquals(initialCalls, reader.reads().size(), "Should not have called reader for cached data");
                assertEquals(data[1000], dest[0]);
            }
        }

        @Test
        void readFullyArrayOutsideCachedRangeDelegatesToReader() throws IOException {
            byte[] data = sequentialBytes(2000);
            RecordingReader reader = new RecordingReader(readerOf(data));
            try (var footer = new CachedFooterProxyReader(reader, 1024)) {
                int initialCalls = reader.reads().size();

                byte[] dest = new byte[10];
                int read = footer.readFully(0, dest, 0, 10);

                assertEquals(10, read);
                assertEquals(initialCalls + 1, reader.reads().size(), "Should have called reader for non-cached data");
                assertEquals(data[0], dest[0]);
            }
        }

        @Test
        void readFullyBufferFromCachedRangeDoesNotCallReader() throws IOException {
            byte[] data = sequentialBytes(2000);
            RecordingReader reader = new RecordingReader(readerOf(data));
            try (var footer = new CachedFooterProxyReader(reader, 1024)) {
                int initialCalls = reader.reads().size();

                ByteBuffer buf = ByteBuffer.allocate(10);
                int read = footer.readFully(1000, buf);

                assertEquals(10, read);
                assertEquals(initialCalls, reader.reads().size(), "Should not have called reader for cached data");
                buf.flip();
                assertEquals(data[1000], buf.get());
            }
        }

        @Test
        void readFullyBufferOutsideCachedRangeDelegatesToReader() throws IOException {
            byte[] data = sequentialBytes(2000);
            RecordingReader reader = new RecordingReader(readerOf(data));
            try (var footer = new CachedFooterProxyReader(reader, 1024)) {
                int initialCalls = reader.reads().size();

                ByteBuffer buf = ByteBuffer.allocate(10);
                int read = footer.readFully(0, buf);

                assertEquals(10, read);
                assertEquals(initialCalls + 1, reader.reads().size(), "Should have called reader for non-cached data");
                buf.flip();
                assertEquals(data[0], buf.get());
            }
        }

        @Test
        void closeClosesInjectedReader() throws IOException {
            RecordingReader reader = new RecordingReader(readerOf(sequentialBytes(2000)));
            var footer = new CachedFooterProxyReader(reader);
            footer.close();
            assertEquals(1, reader.closeCount(), "Injected reader should be closed");
        }

        /**
         * Executor threads of a stream may still read the cache while the stream is
         * being closed: closing must not invalidate it.
         */
        @Test
        void cachedReadsStillWorkAfterClose() throws IOException {
            byte[] data = sequentialBytes(2000);
            var footer = new CachedFooterProxyReader(readerOf(data), 1024);
            footer.close();

            byte[] buf = new byte[10];
            assertEquals(10, footer.readFully(1500, buf, 0, 10));
            assertEquals(data[1500], buf[0]);
            assertTrue(footer.isCached(1500, 10));
        }
    }
}
