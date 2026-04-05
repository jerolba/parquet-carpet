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

import static com.jerolba.carpet.io.s3.S3ContainerHelper.BUCKET_NAME;
import static com.jerolba.carpet.io.s3.S3ContainerHelper.createS3ClientWithBucket;
import static com.jerolba.carpet.io.s3.S3ContainerHelper.createS3LocalStackContainer;
import static com.jerolba.carpet.io.s3.S3ContainerHelper.stopLocalStack;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3SeekableReaderTest {

    private static final String OBJECT_KEY = "test-object.parquet";

    private static LocalStackContainer localStack;
    private static S3Client s3Client;
    private static byte[] testData;

    @BeforeAll
    static void setUp() {
        localStack = createS3LocalStackContainer();
        s3Client = createS3ClientWithBucket(localStack);

        // Create test data with sequential bytes
        testData = new byte[1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }

        putObject(OBJECT_KEY, testData);
    }

    @AfterAll
    static void tearDown() {
        stopLocalStack(localStack);
    }

    @Test
    void getLengthReturnsConfiguredLength() throws IOException {
        try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
            assertEquals(testData.length, reader.getLength());
        }
    }

    @Nested
    class ReadFullyByteArray {

        @Test
        void readAllBytes() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                byte[] buffer = new byte[testData.length];
                int read = reader.readFully(0, buffer, 0, testData.length);

                assertEquals(testData.length, read);
                assertArrayEquals(testData, buffer);
            }
        }

        @Test
        void readFromBeginning() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                byte[] buffer = new byte[100];
                int read = reader.readFully(0, buffer, 0, 100);

                assertEquals(100, read);
                byte[] expected = new byte[100];
                System.arraycopy(testData, 0, expected, 0, 100);
                assertArrayEquals(expected, buffer);
            }
        }

        @Test
        void readFromMiddle() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                byte[] buffer = new byte[200];
                int read = reader.readFully(500, buffer, 0, 200);

                assertEquals(200, read);
                byte[] expected = new byte[200];
                System.arraycopy(testData, 500, expected, 0, 200);
                assertArrayEquals(expected, buffer);
            }
        }

        @Test
        void readFromEnd() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                byte[] buffer = new byte[100];
                int read = reader.readFully(testData.length - 100, buffer, 0, 100);

                assertEquals(100, read);
                byte[] expected = new byte[100];
                System.arraycopy(testData, testData.length - 100, expected, 0, 100);
                assertArrayEquals(expected, buffer);
            }
        }

        @Test
        void readWithOffset() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                byte[] buffer = new byte[150];
                int read = reader.readFully(0, buffer, 50, 100);

                assertEquals(100, read);
                // First 50 bytes should be untouched (zeros)
                for (int i = 0; i < 50; i++) {
                    assertEquals(0, buffer[i]);
                }
                // Bytes 50-149 should contain data from position 0
                byte[] expected = new byte[100];
                System.arraycopy(testData, 0, expected, 0, 100);
                byte[] actual = new byte[100];
                System.arraycopy(buffer, 50, actual, 0, 100);
                assertArrayEquals(expected, actual);
            }
        }

        @Test
        void readSingleByte() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                byte[] buffer = new byte[1];
                int read = reader.readFully(0, buffer, 0, 1);

                assertEquals(1, read);
                assertEquals(testData[0], buffer[0]);
            }
        }

        @Test
        void multipleReadsFromDifferentPositions() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                byte[] buffer1 = new byte[50];
                byte[] buffer2 = new byte[50];
                reader.readFully(0, buffer1, 0, 50);
                reader.readFully(100, buffer2, 0, 50);

                byte[] expected1 = new byte[50];
                byte[] expected2 = new byte[50];
                System.arraycopy(testData, 0, expected1, 0, 50);
                System.arraycopy(testData, 100, expected2, 0, 50);
                assertArrayEquals(expected1, buffer1);
                assertArrayEquals(expected2, buffer2);
            }
        }
    }

    @Nested
    class ReadFullyByteBuffer {

        @Test
        void readAllBytes() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(testData.length);
                int read = reader.readFully(0, byteBuffer);

                assertEquals(testData.length, read);
                byteBuffer.flip();
                byte[] result = new byte[testData.length];
                byteBuffer.get(result);
                assertArrayEquals(testData, result);
            }
        }

        @Test
        void readFromMiddle() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(200);
                int read = reader.readFully(500, byteBuffer);

                assertEquals(200, read);
                byteBuffer.flip();
                byte[] result = new byte[200];
                byteBuffer.get(result);
                byte[] expected = new byte[200];
                System.arraycopy(testData, 500, expected, 0, 200);
                assertArrayEquals(expected, result);
            }
        }

        @Test
        void readFromEnd() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(100);
                int read = reader.readFully(testData.length - 100, byteBuffer);

                assertEquals(100, read);
                byteBuffer.flip();
                byte[] result = new byte[100];
                byteBuffer.get(result);
                byte[] expected = new byte[100];
                System.arraycopy(testData, testData.length - 100, expected, 0, 100);
                assertArrayEquals(expected, result);
            }
        }

        @Test
        void readWithLimitedBuffer() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(200);
                byteBuffer.position(50);
                byteBuffer.limit(150);
                // remaining = 100
                int read = reader.readFully(0, byteBuffer);

                assertEquals(100, read);
                byteBuffer.flip();
                // Skip the first 50 bytes (before position was set)
                byteBuffer.position(50);
                byte[] result = new byte[100];
                byteBuffer.get(result);
                byte[] expected = new byte[100];
                System.arraycopy(testData, 0, expected, 0, 100);
                assertArrayEquals(expected, result);
            }
        }

        @Test
        void readSingleByte() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(1);
                int read = reader.readFully(0, byteBuffer);

                assertEquals(1, read);
                byteBuffer.flip();
                assertEquals(testData[0], byteBuffer.get());
            }
        }

        @Test
        void multipleReadsFromDifferentPositions() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                ByteBuffer buffer1 = ByteBuffer.allocate(50);
                ByteBuffer buffer2 = ByteBuffer.allocate(50);
                reader.readFully(0, buffer1);
                reader.readFully(100, buffer2);

                buffer1.flip();
                buffer2.flip();

                byte[] result1 = new byte[50];
                byte[] result2 = new byte[50];
                buffer1.get(result1);
                buffer2.get(result2);

                byte[] expected1 = new byte[50];
                byte[] expected2 = new byte[50];
                System.arraycopy(testData, 0, expected1, 0, 50);
                System.arraycopy(testData, 100, expected2, 0, 50);
                assertArrayEquals(expected1, result1);
                assertArrayEquals(expected2, result2);
            }
        }
    }

    @Nested
    class ReadFromNonExistentObject {

        @Test
        void readFromNonExistentKeyThrowsIOException() {
            var reader = new S3SeekableReader(s3Client, BUCKET_NAME, "non-existent-key");
            byte[] buffer = new byte[10];

            assertThrows(IOException.class, () -> reader.readFully(0, buffer, 0, 10));
        }

        @Test
        void readByteBufferFromNonExistentKeyThrowsIOException() {
            var reader = new S3SeekableReader(s3Client, BUCKET_NAME, "non-existent-key");
            ByteBuffer byteBuffer = ByteBuffer.allocate(10);

            assertThrows(IOException.class, () -> reader.readFully(0, byteBuffer));
        }
    }

    @Test
    void closeDoesNotThrow() throws IOException {
        var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY);
        reader.close(); // Should not throw
    }

    @Nested
    class DownloadToLocalFile {

        @Test
        void downloadsObjectToTempDirectory() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                Path localPath = reader.downloadToLocalFile();

                assertTrue(Files.exists(localPath));
                assertEquals(testData.length, Files.size(localPath));
                assertArrayEquals(testData, Files.readAllBytes(localPath));
            }
        }

        @Test
        void fileNameUsesKeyBaseNameAndEtag() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                Path localPath = reader.downloadToLocalFile();

                String fileName = localPath.getFileName().toString();
                assertTrue(fileName.startsWith("test-object_"),
                        "Expected file name to start with 'test-object_' but was: " + fileName);
                assertTrue(fileName.endsWith(".parquet"),
                        "Expected file name to end with '.parquet' but was: " + fileName);
            }
        }

        @Test
        void returnsSamePathWhenCalledTwice() throws IOException {
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, OBJECT_KEY)) {
                Path firstPath = reader.downloadToLocalFile();
                Path secondPath = reader.downloadToLocalFile();

                assertEquals(firstPath, secondPath);
            }
        }

        @Test
        void usesCachedFileWhenAlreadyDownloaded() throws IOException {
            // Use a dedicated S3 object so its cache file is isolated from other tests
            String testKey = "cache-check-object.parquet";
            putObject(testKey, testData);

            // Download to establish the cached file
            Path cachedPath;
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, testKey)) {
                cachedPath = reader.downloadToLocalFile();
            }

            // Overwrite the cached file with zeroed data of the same size
            byte[] modifiedData = new byte[testData.length];
            Files.write(cachedPath, modifiedData);

            // A new reader should return the cached (modified) file without re-downloading
            // because the file exists and has the expected size. Overwritten just for
            // testing purpose, in real scenario the file would be unchanged and contain
            // original data.
            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, testKey)) {
                Path result = reader.downloadToLocalFile();

                assertEquals(cachedPath, result);
                assertArrayEquals(modifiedData, Files.readAllBytes(result));
            }
        }

        @Test
        void throwsIOExceptionForNonExistentObject() {
            var reader = new S3SeekableReader(s3Client, BUCKET_NAME, "non-existent-key");
            assertThrows(IOException.class, reader::downloadToLocalFile);
        }

        @Test
        void fileNameUsesOnlyLastSegmentOfKeyWithPath() throws IOException {
            String keyWithPath = "path/to/nested-object.parquet";
            putObject(keyWithPath, testData);

            try (var reader = new S3SeekableReader(s3Client, BUCKET_NAME, keyWithPath)) {
                Path localPath = reader.downloadToLocalFile();

                String fileName = localPath.getFileName().toString();
                assertTrue(fileName.startsWith("nested-object_"),
                        "Expected file name to start with 'nested-object_' but was: " + fileName);
                assertTrue(fileName.endsWith(".parquet"),
                        "Expected file name to end with '.parquet' but was: " + fileName);
                assertArrayEquals(testData, Files.readAllBytes(localPath));
            }
        }
    }

    private static void putObject(String key, byte[] data) {
        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(key)
                        .build(),
                RequestBody.fromBytes(data));
    }
}
