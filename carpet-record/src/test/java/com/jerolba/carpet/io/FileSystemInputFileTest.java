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
package com.jerolba.carpet.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for FileSystemInputFile and its inner SeekableFileInputStream
 * class.
 */
class FileSystemInputFileTest {

    @TempDir
    static Path tempDir;

    static Path testFile;
    static byte[] testData;

    @BeforeAll
    static void setUp() throws IOException {
        testFile = tempDir.resolve("test-file.bin");
        // Create test data with recognizable pattern
        testData = new byte[1024 * 1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Files.write(testFile, testData);
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (testFile != null && Files.exists(testFile)) {
            Files.deleteIfExists(testFile);
        }
    }

    @Test
    void testConstructorWithPath() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        assertEquals(testData.length, inputFile.getLength());
    }

    @Test
    void testConstructorWithFile() throws IOException {
        File file = testFile.toFile();
        FileSystemInputFile inputFile = new FileSystemInputFile(file);
        assertEquals(testData.length, inputFile.getLength());
    }

    @Test
    void testGetLength() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        assertEquals(testData.length, inputFile.getLength());
    }

    @Test
    void testNewStream() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            assertNotNull(stream);
            assertEquals(0, stream.getPos());
        }
    }

    @Test
    void testReadSingleByte() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            int firstByte = stream.read();
            assertEquals(testData[0] & 0xFF, firstByte);
            assertEquals(1, stream.getPos());
        }
    }

    @Test
    void testReadByteArray() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            byte[] buffer = new byte[100];
            int bytesRead = stream.read(buffer);
            assertEquals(100, bytesRead);
            for (int i = 0; i < 100; i++) {
                assertEquals(testData[i], buffer[i], "Byte at position " + i + " should match");
            }
            assertEquals(100, stream.getPos());
        }
    }

    @Test
    void testReadByteArrayWithOffset() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            byte[] buffer = new byte[150];
            int bytesRead = stream.read(buffer, 50, 100);
            assertEquals(100, bytesRead);
            for (int i = 0; i < 100; i++) {
                assertEquals(testData[i], buffer[50 + i], "Byte at position " + i + " should match");
            }
            assertEquals(100, stream.getPos());
        }
    }

    @Test
    void testReadByteBuffer() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(100);
            int bytesRead = stream.read(buffer);
            assertEquals(100, bytesRead);
            buffer.flip();
            for (int i = 0; i < 100; i++) {
                assertEquals(testData[i], buffer.get(i), "Byte at position " + i + " should match");
            }
            assertEquals(100, stream.getPos());
        }
    }

    @Test
    void testReadFullyByteArray() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            byte[] buffer = new byte[100];
            stream.readFully(buffer);
            for (int i = 0; i < 100; i++) {
                assertEquals(testData[i], buffer[i], "Byte at position " + i + " should match");
            }
            assertEquals(100, stream.getPos());
        }
    }

    @Test
    void testReadFullyByteArrayWithOffsetAndLength() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            byte[] buffer = new byte[150];
            stream.readFully(buffer, 50, 100);
            for (int i = 0; i < 100; i++) {
                assertEquals(testData[i], buffer[50 + i], "Byte at position " + i + " should match");
            }
            assertEquals(100, stream.getPos());
        }
    }

    @Test
    void testReadFullyByteBuffer() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(100);
            stream.readFully(buffer);
            buffer.flip();
            for (int i = 0; i < 100; i++) {
                assertEquals(testData[i], buffer.get(i), "Byte at position " + i + " should match");
            }
            assertEquals(100, stream.getPos());
        }
    }

    @Test
    void testReadFullyThrowsExceptionOnEOF() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(testData.length - 50);
            byte[] buffer = new byte[100];
            assertThrows(IOException.class, () -> stream.readFully(buffer));
        }
    }

    @Test
    void testReadFullyByteBufferThrowsExceptionOnEOF() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(testData.length - 50);
            ByteBuffer buffer = ByteBuffer.allocate(100);
            assertThrows(IOException.class, () -> stream.readFully(buffer));
        }
    }

    @Test
    void testSeekAndGetPos() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(100);
            assertEquals(100, stream.getPos());

            stream.seek(500);
            assertEquals(500, stream.getPos());

            stream.seek(0);
            assertEquals(0, stream.getPos());
        }
    }

    @Test
    void testSeekAndRead() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(100);
            int value = stream.read();
            assertEquals(testData[100] & 0xFF, value);
            assertEquals(101, stream.getPos());
        }
    }

    @Test
    void testSkip() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            long skipped = stream.skip(100);
            assertEquals(100, skipped);
            assertEquals(100, stream.getPos());

            int value = stream.read();
            assertEquals(testData[100] & 0xFF, value);
        }
    }

    @Test
    void testSkipZeroOrNegative() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            long skipped = stream.skip(0);
            assertEquals(0, skipped);
            assertEquals(0, stream.getPos());

            skipped = stream.skip(-10);
            assertEquals(0, skipped);
            assertEquals(0, stream.getPos());
        }
    }

    @Test
    void testSkipBeyondEndOfFile() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(testData.length - 10);
            long skipped = stream.skip(100);
            assertEquals(10, skipped);
            assertEquals(testData.length, stream.getPos());
        }
    }

    @Test
    void testSkipAtEndOfFile() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(testData.length);
            long skipped = stream.skip(100);
            assertEquals(0, skipped);
            assertEquals(testData.length, stream.getPos());
        }
    }

    @Test
    void testAvailable() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            assertEquals(testData.length, stream.available());

            stream.seek(100);
            assertEquals(testData.length - 100, stream.available());

            stream.seek(testData.length);
            assertEquals(0, stream.available());
        }
    }

    @Test
    void testMarkAndReset() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(100);
            stream.mark(0);

            stream.seek(200);
            assertEquals(200, stream.getPos());

            stream.reset();
            assertEquals(100, stream.getPos());
        }
    }

    @Test
    void testMarkSupported() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            assertTrue(stream.markSupported());
        }
    }

    @Test
    void testMarkAndResetMultipleTimes() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(100);
            stream.mark(0);

            stream.seek(200);
            stream.reset();
            assertEquals(100, stream.getPos());

            stream.seek(300);
            stream.mark(0);

            stream.seek(400);
            stream.reset();
            assertEquals(300, stream.getPos());
        }
    }

    @Test
    void testReadAtEndOfFile() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(testData.length);
            int value = stream.read();
            assertEquals(-1, value);
        }
    }

    @Test
    void testReadByteArrayAtEndOfFile() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(testData.length);
            byte[] buffer = new byte[100];
            int bytesRead = stream.read(buffer);
            assertEquals(-1, bytesRead);
        }
    }

    @Test
    void testReadByteBufferAtEndOfFile() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(testData.length);
            ByteBuffer buffer = ByteBuffer.allocate(100);
            int bytesRead = stream.read(buffer);
            assertEquals(-1, bytesRead);
        }
    }

    @Test
    void testPartialReadAtEndOfFile() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            stream.seek(testData.length - 10);
            byte[] buffer = new byte[100];
            int bytesRead = stream.read(buffer);
            assertEquals(10, bytesRead);
            for (int i = 0; i < 10; i++) {
                assertEquals(testData[testData.length - 10 + i], buffer[i]);
            }
        }
    }

    @Test
    void testSequentialReads() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            byte[] buffer = new byte[100];

            // First read
            int bytesRead = stream.read(buffer);
            assertEquals(100, bytesRead);
            for (int i = 0; i < 100; i++) {
                assertEquals(testData[i], buffer[i]);
            }

            // Second read
            bytesRead = stream.read(buffer);
            assertEquals(100, bytesRead);
            for (int i = 0; i < 100; i++) {
                assertEquals(testData[100 + i], buffer[i]);
            }

            assertEquals(200, stream.getPos());
        }
    }

    @Test
    void testEmptyFile() throws IOException {
        Path emptyFile = tempDir.resolve("empty-file.bin");
        Files.write(emptyFile, new byte[0]);

        FileSystemInputFile inputFile = new FileSystemInputFile(emptyFile);
        assertEquals(0, inputFile.getLength());

        try (SeekableInputStream stream = inputFile.newStream()) {
            assertEquals(0, stream.available());
            assertEquals(-1, stream.read());
        }
    }

    @Test
    void testLargeByteBufferRead() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(testData.length);
            int bytesRead = stream.read(buffer);
            assertEquals(testData.length, bytesRead);
            buffer.flip();
            for (int i = 0; i < testData.length; i++) {
                assertEquals(testData[i], buffer.get(i));
            }
        }
    }

    @Test
    void testMultipleStreams() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);

        try (SeekableInputStream stream1 = inputFile.newStream();
                SeekableInputStream stream2 = inputFile.newStream()) {

            stream1.seek(100);
            stream2.seek(200);

            assertEquals(100, stream1.getPos());
            assertEquals(200, stream2.getPos());

            assertEquals(testData[100] & 0xFF, stream1.read());
            assertEquals(testData[200] & 0xFF, stream2.read());
        }
    }

    @Test
    void testCloseStream() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        SeekableInputStream stream = inputFile.newStream();
        stream.read();
        stream.close();

        // Verify that operations after close throw exception
        assertThrows(IOException.class, () -> stream.read());
    }

    @Test
    void testGetLengthFromStream() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            // Test that we can seek to the file length and position is correctly reported
            assertEquals(0, stream.getPos());

            // Seek to the end of the file
            stream.seek(testData.length);
            assertEquals(testData.length, stream.getPos());

            // Verify available bytes is 0 at end
            assertEquals(0, stream.available());
        }
    }

    @Test
    void testNonExistentFile() {
        Path nonExistentFile = tempDir.resolve("non-existent-file.bin");
        FileSystemInputFile inputFile = new FileSystemInputFile(nonExistentFile);

        assertThrows(IOException.class, () -> inputFile.getLength());
        assertThrows(IOException.class, () -> inputFile.newStream());
    }

    @Test
    void testReadByteArray1KFromOffset() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            int offset = 50000; // Start from middle of file
            int size = 1024; // 1K
            stream.seek(offset);

            byte[] buffer = new byte[size];
            int bytesRead = stream.read(buffer);
            assertEquals(size, bytesRead);

            for (int i = 0; i < size; i++) {
                assertEquals(testData[offset + i], buffer[i], "Byte at position " + (offset + i) + " should match");
            }
            assertEquals(offset + size, stream.getPos());
        }
    }

    @Test
    void testReadByteArray10KFromOffset() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            int offset = 100000; // Start from later in file
            int size = 10240; // 10K
            stream.seek(offset);

            byte[] buffer = new byte[size];
            int bytesRead = stream.read(buffer);
            assertEquals(size, bytesRead);

            for (int i = 0; i < size; i++) {
                assertEquals(testData[offset + i], buffer[i], "Byte at position " + (offset + i) + " should match");
            }
            assertEquals(offset + size, stream.getPos());
        }
    }

    @Test
    void testReadByteArray100KFromOffset() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            int offset = 200000; // Start from later in file
            int size = 102400; // 100K
            stream.seek(offset);

            byte[] buffer = new byte[size];
            int bytesRead = stream.read(buffer);
            assertEquals(size, bytesRead);

            for (int i = 0; i < size; i++) {
                assertEquals(testData[offset + i], buffer[i], "Byte at position " + (offset + i) + " should match");
            }
            assertEquals(offset + size, stream.getPos());
        }
    }

    @Test
    void testReadByteBuffer1KFromOffset() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            int offset = 75000; // Start from different position
            int size = 1024; // 1K
            stream.seek(offset);

            ByteBuffer buffer = ByteBuffer.allocate(size);
            int bytesRead = stream.read(buffer);
            assertEquals(size, bytesRead);

            buffer.flip();
            for (int i = 0; i < size; i++) {
                assertEquals(testData[offset + i], buffer.get(i), "Byte at position " + (offset + i) + " should match");
            }
            assertEquals(offset + size, stream.getPos());
        }
    }

    @Test
    void testReadByteBuffer10KFromOffset() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            int offset = 150000; // Start from different position
            int size = 10240; // 10K
            stream.seek(offset);

            ByteBuffer buffer = ByteBuffer.allocate(size);
            int bytesRead = stream.read(buffer);
            assertEquals(size, bytesRead);

            buffer.flip();
            for (int i = 0; i < size; i++) {
                assertEquals(testData[offset + i], buffer.get(i), "Byte at position " + (offset + i) + " should match");
            }
            assertEquals(offset + size, stream.getPos());
        }
    }

    @Test
    void testReadByteBuffer100KFromOffset() throws IOException {
        FileSystemInputFile inputFile = new FileSystemInputFile(testFile);
        try (SeekableInputStream stream = inputFile.newStream()) {
            int offset = 300000; // Start from different position
            int size = 102400; // 100K
            stream.seek(offset);

            ByteBuffer buffer = ByteBuffer.allocate(size);
            int bytesRead = stream.read(buffer);
            assertEquals(size, bytesRead);

            buffer.flip();
            for (int i = 0; i < size; i++) {
                assertEquals(testData[offset + i], buffer.get(i), "Byte at position " + (offset + i) + " should match");
            }
            assertEquals(offset + size, stream.getPos());
        }
    }
}