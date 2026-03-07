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
package com.jerolba.carpet.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for FileSystemOutputFile and CountedPositionOutputStream classes.
 */
class FileSystemOutputFileTest {

    @TempDir
    static Path tempDir;

    static byte[] testData;

    @BeforeAll
    static void setUp() throws IOException {
        // Create test data with recognizable pattern
        testData = new byte[1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
    }

    @Nested
    class FileCreation {

        private final Path testFile = tempDir.resolve("test-output-file.bin");

        @AfterEach
        void tearDown() throws IOException {
            if (testFile != null && Files.exists(testFile)) {
                Files.deleteIfExists(testFile);
            }
        }

        @Test
        void testConstructorWithPath() {
            FileSystemOutputFile outputFile = new FileSystemOutputFile(testFile);
            assertEquals(testFile.toString(), outputFile.getPath());
        }

        @Test
        void testConstructorWithFile() {
            File file = testFile.toFile();
            FileSystemOutputFile outputFile = new FileSystemOutputFile(file);
            assertEquals(testFile.toString(), outputFile.getPath());
        }

        @Test
        void testGetPath() {
            FileSystemOutputFile outputFile = new FileSystemOutputFile(testFile);
            assertEquals(testFile.toString(), outputFile.getPath());
        }

        @Test
        void testCreateFailsIfFileExists() throws IOException {
            // Create a file first
            Files.write(testFile, new byte[] { 1, 2, 3 });
            assertTrue(Files.exists(testFile));

            FileSystemOutputFile outputFile = new FileSystemOutputFile(testFile);

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> outputFile.create(1024));
            assertTrue(exception.getMessage().contains("Path already exists"));
        }

        @Test
        void testCreateOrOverwriteExistingFile() throws IOException {
            // Create initial file with some content
            Files.write(testFile, new byte[] { 1, 2, 3, 4, 5 });
            assertTrue(Files.exists(testFile));
            assertEquals(5, Files.size(testFile));

            FileSystemOutputFile outputFile = new FileSystemOutputFile(testFile);

            try (PositionOutputStream stream = outputFile.createOrOverwrite(1024)) {
                assertNotNull(stream);
                assertEquals(0, stream.getPos());

                // Write new content
                stream.write(testData, 0, 200);
                assertEquals(200, stream.getPos());
            }

            // File should be overwritten with new content
            assertTrue(Files.exists(testFile));
            assertEquals(200, Files.size(testFile));

            // Verify content
            byte[] writtenData = Files.readAllBytes(testFile);
            for (int i = 0; i < 200; i++) {
                assertEquals(testData[i], writtenData[i]);
            }
        }
    }

    @Test
    void testCreateNewFile() throws IOException {
        Path newFile = tempDir.resolve("new-file.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(newFile);

        try (PositionOutputStream stream = outputFile.create(1024)) {
            assertNotNull(stream);
            assertEquals(0, stream.getPos());
            assertTrue(Files.exists(newFile)); // File is created immediately by Files.newOutputStream()

            stream.write(42);
            assertEquals(1, stream.getPos());
        }

        // File should exist after closing
        assertTrue(Files.exists(newFile));
        assertEquals(1, Files.size(newFile));

        // Clean up
        Files.deleteIfExists(newFile);
    }

    @Test
    void testCreateOrOverwriteNewFile() throws IOException {
        Path newFile = tempDir.resolve("overwrite-new-file.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(newFile);

        try (PositionOutputStream stream = outputFile.createOrOverwrite(1024)) {
            assertNotNull(stream);
            assertEquals(0, stream.getPos());

            stream.write(testData, 0, 100);
            assertEquals(100, stream.getPos());
        }

        assertTrue(Files.exists(newFile));
        assertEquals(100, Files.size(newFile));

        // Clean up
        Files.deleteIfExists(newFile);
    }

    @Test
    void testCountedPositionOutputStreamWriteInt() throws IOException {
        Path testStreamFile = tempDir.resolve("stream-int.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(testStreamFile);

        try (PositionOutputStream stream = outputFile.createOrOverwrite(1024)) {
            assertEquals(0, stream.getPos());

            stream.write(255); // Write single byte
            assertEquals(1, stream.getPos());

            stream.write(128);
            assertEquals(2, stream.getPos());

            stream.write(0);
            assertEquals(3, stream.getPos());
        }

        assertEquals(3, Files.size(testStreamFile));
        byte[] data = Files.readAllBytes(testStreamFile);
        assertEquals((byte) 255, data[0]);
        assertEquals((byte) 128, data[1]);
        assertEquals((byte) 0, data[2]);

        Files.deleteIfExists(testStreamFile);
    }

    @Test
    void testCountedPositionOutputStreamWriteByteArray() throws IOException {
        Path testStreamFile = tempDir.resolve("stream-array.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(testStreamFile);

        try (PositionOutputStream stream = outputFile.createOrOverwrite(1024)) {
            assertEquals(0, stream.getPos());

            byte[] data = { 10, 20, 30, 40, 50 };
            stream.write(data);
            assertEquals(5, stream.getPos());

            stream.write(data, 1, 3); // Write 20, 30, 40
            assertEquals(8, stream.getPos());
        }

        assertEquals(8, Files.size(testStreamFile));
        byte[] writtenData = Files.readAllBytes(testStreamFile);
        assertEquals(10, writtenData[0]);
        assertEquals(20, writtenData[1]);
        assertEquals(30, writtenData[2]);
        assertEquals(40, writtenData[3]);
        assertEquals(50, writtenData[4]);
        assertEquals(20, writtenData[5]);
        assertEquals(30, writtenData[6]);
        assertEquals(40, writtenData[7]);

        Files.deleteIfExists(testStreamFile);
    }

    @Test
    void testCountedPositionOutputStreamWriteByteArrayWithOffset() throws IOException {
        Path testStreamFile = tempDir.resolve("stream-offset.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(testStreamFile);

        try (PositionOutputStream stream = outputFile.createOrOverwrite(1024)) {
            assertEquals(0, stream.getPos());

            byte[] data = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            stream.write(data, 2, 5); // Write 3, 4, 5, 6, 7
            assertEquals(5, stream.getPos());

            stream.write(data, 0, 3); // Write 1, 2, 3
            assertEquals(8, stream.getPos());
        }

        assertEquals(8, Files.size(testStreamFile));
        byte[] writtenData = Files.readAllBytes(testStreamFile);
        assertEquals(3, writtenData[0]);
        assertEquals(4, writtenData[1]);
        assertEquals(5, writtenData[2]);
        assertEquals(6, writtenData[3]);
        assertEquals(7, writtenData[4]);
        assertEquals(1, writtenData[5]);
        assertEquals(2, writtenData[6]);
        assertEquals(3, writtenData[7]);

        Files.deleteIfExists(testStreamFile);
    }

    @Test
    void testCountedPositionOutputStreamLargeData() throws IOException {
        Path testStreamFile = tempDir.resolve("stream-large.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(testStreamFile);

        try (PositionOutputStream stream = outputFile.createOrOverwrite(1024)) {
            assertEquals(0, stream.getPos());

            // Write first 512 bytes
            stream.write(testData, 0, 512);
            assertEquals(512, stream.getPos());

            // Write next 256 bytes
            stream.write(testData, 512, 256);
            assertEquals(768, stream.getPos());

            // Write remaining bytes
            stream.write(testData, 768, 256);
            assertEquals(1024, stream.getPos());
        }

        assertEquals(1024, Files.size(testStreamFile));
        byte[] writtenData = Files.readAllBytes(testStreamFile);
        for (int i = 0; i < 1024; i++) {
            assertEquals(testData[i], writtenData[i], "Byte at position " + i + " should match");
        }

        Files.deleteIfExists(testStreamFile);
    }

    @Test
    void testCountedPositionOutputStreamFlush() throws IOException {
        Path testStreamFile = tempDir.resolve("stream-flush.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(testStreamFile);

        try (PositionOutputStream stream = outputFile.createOrOverwrite(1024)) {
            stream.write(42);
            stream.flush(); // Should not throw exception

            stream.write(testData, 0, 100);
            stream.flush(); // Should not throw exception
        }

        assertTrue(Files.exists(testStreamFile));
        assertEquals(101, Files.size(testStreamFile));

        Files.deleteIfExists(testStreamFile);
    }

    @Test
    void testCountedPositionOutputStreamClose() throws IOException {
        Path testStreamFile = tempDir.resolve("stream-close.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(testStreamFile);

        PositionOutputStream stream = outputFile.createOrOverwrite(1024);
        stream.write(42);
        stream.close(); // Should not throw exception

        assertTrue(Files.exists(testStreamFile));
        assertEquals(1, Files.size(testStreamFile));

        Files.deleteIfExists(testStreamFile);
    }

    @Test
    void testCountedPositionOutputStreamOperationsAfterClose() throws IOException {
        Path testStreamFile = tempDir.resolve("stream-after-close.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(testStreamFile);

        PositionOutputStream stream = outputFile.createOrOverwrite(1024);
        stream.write(42);
        long posAfterWrite = stream.getPos();
        stream.close();

        // getPos should still work after close
        assertEquals(posAfterWrite, stream.getPos());

        Files.deleteIfExists(testStreamFile);
    }

    @Test
    void testCountedPositionOutputStreamMixedOperations() throws IOException {
        Path testStreamFile = tempDir.resolve("stream-mixed.bin");
        FileSystemOutputFile outputFile = new FileSystemOutputFile(testStreamFile);

        try (PositionOutputStream stream = outputFile.createOrOverwrite(1024)) {
            assertEquals(0, stream.getPos());

            // Mix different write operations
            stream.write(1); // pos = 1
            assertEquals(1, stream.getPos());

            stream.write(testData, 0, 10); // pos = 11
            assertEquals(11, stream.getPos());

            stream.write(2); // pos = 12
            assertEquals(12, stream.getPos());

            stream.write(testData, 10, 20); // pos = 32
            assertEquals(32, stream.getPos());

            byte[] smallArray = { 100, 101, 102 };
            stream.write(smallArray); // pos = 35
            assertEquals(35, stream.getPos());
        }

        assertEquals(35, Files.size(testStreamFile));
        byte[] writtenData = Files.readAllBytes(testStreamFile);

        // Verify the mixed writes
        assertEquals(1, writtenData[0]);
        for (int i = 0; i < 10; i++) {
            assertEquals(testData[i], writtenData[i + 1]);
        }
        assertEquals(2, writtenData[11]);
        for (int i = 0; i < 20; i++) {
            assertEquals(testData[i + 10], writtenData[i + 12]);
        }
        assertEquals(100, writtenData[32]);
        assertEquals(101, writtenData[33]);
        assertEquals(102, writtenData[34]);

        Files.deleteIfExists(testStreamFile);
    }
}