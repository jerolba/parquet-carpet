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

import static com.jerolba.carpet.io.s3.S3ContainerHelper.BUCKET_NAME;
import static com.jerolba.carpet.io.s3.S3ContainerHelper.createS3ClientWithBucket;
import static com.jerolba.carpet.io.s3.S3ContainerHelper.createS3LocalStackContainer;
import static com.jerolba.carpet.io.s3.S3ContainerHelper.stopLocalStack;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.localstack.LocalStackContainer;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.io.FileSystemOutputFile;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3InputFileTest {

    private static LocalStackContainer localStack;

    @BeforeAll
    static void setUp() {
        localStack = createS3LocalStackContainer();
        createS3ClientWithBucket(localStack);
    }

    @AfterAll
    static void tearDown() {
        stopLocalStack(localStack);
    }

    private static <T> byte[] writeParquetBytes(Class<T> recordClass, List<T> records) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var writer = new CarpetWriter<>(baos, recordClass)) {
            writer.write(records);
        }
        return baos.toByteArray();
    }

    private static void uploadToS3(String url, byte[] data) {
        String bucket = BUCKET_NAME;
        String key = url;
        if (url.startsWith("s3://")) {
            String path = url.substring(5);
            int slashIndex = path.indexOf('/');
            bucket = path.substring(0, slashIndex);
            key = path.substring(slashIndex + 1);
        }
        try (S3Client s3Client = S3Client.create()) {
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build(),
                    RequestBody.fromBytes(data));
        }
    }

    record SimpleRecord(long id, String name, double value) {
    }

    record WideRecord(long id, String name, double value, int count, String description) {
    }

    record NarrowRecord(long id, String name) {
    }

    @Nested
    class GetLength {

        @Test
        void getLengthReturnsCorrectSize() throws IOException {
            var records = List.of(new SimpleRecord(1, "one", 1.0));
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, records);
            String url = "s3://test-bucket/get-length-correct.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            assertEquals(parquetBytes.length, inputFile.getLength());
        }

        @Test
        void getLengthThrowsForNonExistentKey() throws IOException {
            var inputFile = S3InputFile.builder("s3://test-bucket/non-existent-key.parquet").build();
            var ex = assertThrows(IOException.class, inputFile::getLength);
            assertTrue(ex.getMessage().contains("non-existent-key.parquet"));
        }

        @Test
        void getLengthThrowsForNonExistentBucket() throws IOException {
            var inputFile = S3InputFile.builder().bucket("non-existent-bucket")
                    .key("some-key.parquet").build();
            var ex = assertThrows(IOException.class, inputFile::getLength);
            assertTrue(ex.getMessage().contains("non-existent-bucket"));
        }
    }

    @Nested
    class NewStream {

        @Test
        void newStreamReturnsValidStream() throws IOException {
            var records = List.of(new SimpleRecord(1, "one", 1.0));
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, records);
            String url = "s3://test-bucket/new-stream-valid.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            try (SeekableInputStream stream = inputFile.newStream()) {
                assertNotNull(stream);
                assertEquals(0, stream.getPos());
            }
        }

        @Test
        void newStreamReturnsIndependentInstances() throws IOException {
            var records = List.of(new SimpleRecord(1, "one", 1.0));
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, records);
            String url = "s3://test-bucket/new-stream-independent.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            try (SeekableInputStream stream1 = inputFile.newStream();
                    SeekableInputStream stream2 = inputFile.newStream()) {
                assertNotSame(stream1, stream2);
                stream1.seek(10);
                assertEquals(10, stream1.getPos());
                assertEquals(0, stream2.getPos());
            }
        }
    }

    @Nested
    class ReadRecords {

        @Test
        void readSimpleRecord() throws IOException {
            var expected = List.of(
                    new SimpleRecord(1, "Alice", 10.5),
                    new SimpleRecord(2, "Bob", 20.3));
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, expected);
            String url = "s3://test-bucket/read-simple.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            List<SimpleRecord> actual = new CarpetReader<>(inputFile, SimpleRecord.class).toList();
            assertEquals(expected, actual);
        }

        @Test
        void readMultipleRecords() throws IOException {
            var expected = IntStream.range(0, 100)
                    .mapToObj(i -> new SimpleRecord(i, "name-" + i, i * 1.1))
                    .toList();
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, expected);
            String url = "s3://test-bucket/read-multiple.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            List<SimpleRecord> actual = new CarpetReader<>(inputFile, SimpleRecord.class).toList();
            assertEquals(expected, actual);
        }

        @Test
        void readWithStreamApi() throws IOException {
            var records = IntStream.range(0, 50)
                    .mapToObj(i -> new SimpleRecord(i, "name-" + i, i * 2.0))
                    .toList();
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, records);
            String url = "s3://test-bucket/read-stream-api.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            try (var stream = new CarpetReader<>(inputFile, SimpleRecord.class).stream()) {
                List<SimpleRecord> filtered = stream
                        .filter(r -> r.id() >= 10 && r.id() < 20)
                        .toList();
                assertEquals(10, filtered.size());
                assertEquals(10, filtered.get(0).id());
                assertEquals(19, filtered.get(9).id());
            }
        }

        @Test
        void readEmptyFile() throws IOException {
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, List.of());
            String url = "s3://test-bucket/read-empty.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            List<SimpleRecord> actual = new CarpetReader<>(inputFile, SimpleRecord.class).toList();
            assertTrue(actual.isEmpty());
        }

        @Test
        void readWithProjection() throws IOException {
            var records = List.of(
                    new WideRecord(1, "Alice", 10.5, 42, "First record"),
                    new WideRecord(2, "Bob", 20.3, 99, "Second record"));
            byte[] parquetBytes = writeParquetBytes(WideRecord.class, records);
            String url = "s3://test-bucket/read-projection.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            List<NarrowRecord> actual = new CarpetReader<>(inputFile, NarrowRecord.class).toList();
            assertEquals(2, actual.size());
            assertEquals(new NarrowRecord(1, "Alice"), actual.get(0));
            assertEquals(new NarrowRecord(2, "Bob"), actual.get(1));
        }

        @Test
        void readLargeFile() throws IOException {
            var expected = IntStream.range(0, 1_000_000)
                    .mapToObj(i -> new SimpleRecord(i, "record-" + i, i * 0.01))
                    .toList();
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, expected);
            String url = "s3://test-bucket/read-large.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            List<SimpleRecord> actual = new CarpetReader<>(inputFile, SimpleRecord.class).toList();
            assertEquals(expected.size(), actual.size());
            assertEquals(expected.get(0), actual.get(0));
            assertEquals(expected.get(expected.size() - 1), actual.get(actual.size() - 1));
            assertEquals(expected, actual);
        }
    }

    @Nested
    class ConcurrencyModes {

        @Test
        void readWithConcurrencyOne() throws IOException {
            var expected = IntStream.range(0, 200)
                    .mapToObj(i -> new SimpleRecord(i, "c1-" + i, i * 0.5))
                    .toList();
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, expected);
            String url = "s3://test-bucket/read-concurrency-one.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            List<SimpleRecord> actual = new CarpetReader<>(inputFile, SimpleRecord.class).toList();
            assertEquals(expected, actual);
        }

        @Test
        void readWithConcurrencyMultiple() throws IOException {
            var expected = IntStream.range(0, 200)
                    .mapToObj(i -> new SimpleRecord(i, "cm-" + i, i * 0.5))
                    .toList();
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, expected);
            String url = "s3://test-bucket/read-concurrency-multi.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.builder(url).concurrency(4).build();
            List<SimpleRecord> actual = new CarpetReader<>(inputFile, SimpleRecord.class).toList();
            assertEquals(expected, actual);
        }

        @Test
        void bothConcurrencyModesProduceSameResult() throws IOException {
            var expected = IntStream.range(0, 500)
                    .mapToObj(i -> new SimpleRecord(i, "both-" + i, i * 1.5))
                    .toList();
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, expected);
            String url = "s3://test-bucket/read-concurrency-both.parquet";
            uploadToS3(url, parquetBytes);

            var sequential = S3InputFile.builder(url).concurrency(1).build();
            var parallel = S3InputFile.builder(url).concurrency(4).build();

            List<SimpleRecord> seqResult = new CarpetReader<>(sequential, SimpleRecord.class).toList();
            List<SimpleRecord> parResult = new CarpetReader<>(parallel, SimpleRecord.class).toList();

            assertEquals(seqResult, parResult);
            assertEquals(expected, seqResult);
        }
    }

    @Nested
    class ErrorCases {

        @Test
        void readFromNonExistentKeyThrows() throws IOException {
            var inputFile = S3InputFile.builder("s3://test-bucket/does-not-exist.parquet").build();
            assertThrows(Exception.class, () -> new CarpetReader<>(inputFile, SimpleRecord.class).toList());
        }

        @Test
        void readFromNonExistentBucketThrows() throws IOException {
            var inputFile = S3InputFile.builder("s3://no-such-bucket/some-key.parquet").build();
            assertThrows(Exception.class, () -> new CarpetReader<>(inputFile, SimpleRecord.class).toList());
        }
    }

    @Nested
    class FileSizeScenarios {

        @Test
        void readSmallFile() throws IOException {
            var expected = List.of(new SimpleRecord(1, "small", 1.0));
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, expected);
            // Small parquet file is well under 8 MiB footer cache — entirely cached
            assertTrue(parquetBytes.length < 8 * 1024 * 1024);
            String url = "s3://test-bucket/read-small.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.of(url);
            List<SimpleRecord> actual = new CarpetReader<>(inputFile, SimpleRecord.class).toList();
            assertEquals(expected, actual);
        }

        @Test
        void readSmallFileWithConcurrency() throws IOException {
            var expected = List.of(
                    new SimpleRecord(1, "a", 1.0),
                    new SimpleRecord(2, "b", 2.0));
            byte[] parquetBytes = writeParquetBytes(SimpleRecord.class, expected);
            String url = "s3://test-bucket/read-small-concurrent.parquet";
            uploadToS3(url, parquetBytes);

            var inputFile = S3InputFile.builder(url).concurrency(4).build();
            List<SimpleRecord> actual = new CarpetReader<>(inputFile, SimpleRecord.class).toList();
            assertEquals(expected, actual);
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class LargeFileScenarios {

        private static final int RECORD_COUNT = 400_000;
        private static final String LARGE_FILE_KEY = "large-file.parquet";
        private static final String LARGE_FILE_URL = "s3://test-bucket/" + LARGE_FILE_KEY;

        record HeavyRecord(long id, long value, String payload) {
        }

        record ProjectionRecord(long id, String payload) {
        }

        private final HeavyRecord firstRecord = new HeavyRecord(0, 0, makePayload(0));
        private final HeavyRecord lastRecord = new HeavyRecord(RECORD_COUNT - 1, (RECORD_COUNT - 1) * 2,
                makePayload(RECORD_COUNT - 1));
        private long uploadedSize;

        /**
         * Builds a 2048-char string from a 32-bit LCG. Hex output has ~4 bits of
         * entropy per char, so Snappy achieves very little compression, ensuring the
         * resulting parquet file stays large.
         */
        private static String makePayload(int seed) {
            var sb = new StringBuilder(512 + 10);
            int x = seed;
            while (sb.length() < 512) {
                x = x * 1664525 + 1013904223;
                sb.append(Integer.toHexString(Integer.MAX_VALUE & x));
            }
            return sb.substring(0, 512);
        }

        @BeforeAll
        void setUpLargeFile() throws IOException {
            // Write to a temp file with small row groups to avoid a 128 MiB row-group
            // buffer in heap. Rows are generated lazily and flushed to disk every 8 MiB.
            // createTempFile gives a unique path; delete the empty placeholder so
            // FileSystemOutputFile.create() (which requires a non-existing path) can work.
            Path tempFile = Files.createTempFile("large-parquet-test", ".parquet");
            Files.delete(tempFile);
            try {
                var outputFile = new FileSystemOutputFile(tempFile.toFile());
                try (var writer = new CarpetWriter.Builder<>(outputFile, HeavyRecord.class)
                        .withRowGroupSize(10L * 1024 * 1024)
                        .build()) {
                    var stream = IntStream.range(0, RECORD_COUNT)
                            .mapToObj(i -> new HeavyRecord(i, i * 2, makePayload(i)));
                    writer.write(stream);
                }
                uploadedSize = Files.size(tempFile);
                assertTrue(uploadedSize > 100L * 1024 * 1024,
                        "Expected parquet file > 100 MiB but was " + uploadedSize + " bytes");
                uploadToS3(LARGE_FILE_URL, Files.readAllBytes(tempFile));
            } finally {
                Files.deleteIfExists(tempFile);
            }
        }

        @Test
        void getLengthReturnsCorrectSizeForLargeFile() throws IOException {
            var inputFile = S3InputFile.of(LARGE_FILE_URL);
            assertEquals(uploadedSize, inputFile.getLength());
        }

        @Test
        void readLargeFileWithSequentialReader() throws IOException {
            var inputFile = S3InputFile.builder(LARGE_FILE_URL).concurrency(1).build();
            List<HeavyRecord> actual = new CarpetReader<>(inputFile, HeavyRecord.class).toList();
            assertEquals(RECORD_COUNT, actual.size());
            assertEquals(firstRecord, actual.get(0));
            assertEquals(lastRecord, actual.get(RECORD_COUNT - 1));
        }

        @Test
        void readLargeFileWithConcurrentReader() throws IOException {
            var inputFile = S3InputFile.builder(LARGE_FILE_URL).concurrency(4).build();
            List<HeavyRecord> actual = new CarpetReader<>(inputFile, HeavyRecord.class).toList();
            assertEquals(RECORD_COUNT, actual.size());
            assertEquals(firstRecord, actual.get(0));
            assertEquals(lastRecord, actual.get(RECORD_COUNT - 1));
        }

        @Test
        void readLargeFileWithProjection() throws IOException {
            var inputFile = S3InputFile.builder(LARGE_FILE_URL).concurrency(4).build();
            List<ProjectionRecord> actual = new CarpetReader<>(inputFile, ProjectionRecord.class).toList();
            assertEquals(RECORD_COUNT, actual.size());
            assertEquals(firstRecord.id, actual.get(0).id);
            assertEquals(firstRecord.payload, actual.get(0).payload);
            assertEquals(lastRecord.id, actual.get(RECORD_COUNT - 1).id);
            assertEquals(lastRecord.payload, actual.get(RECORD_COUNT - 1).payload);
        }

        @Test
        void heapAllocatorConfigured() throws IOException {
            var inputFile = S3InputFile.of(LARGE_FILE_URL);
            var allocator = new ByteBufferAllocatorWrapper(HeapByteBufferAllocator.getInstance());
            List<ProjectionRecord> actual = new CarpetReader.Builder<>(inputFile, ProjectionRecord.class)
                    .withAllocator(allocator)
                    .build()
                    .toList();
            assertEquals(RECORD_COUNT, actual.size());
            assertEquals(firstRecord.id, actual.get(0).id);
            assertEquals(firstRecord.payload, actual.get(0).payload);
            assertEquals(lastRecord.id, actual.get(RECORD_COUNT - 1).id);
            assertEquals(lastRecord.payload, actual.get(RECORD_COUNT - 1).payload);
            assertEquals(22, allocator.allocationCount(),
                    "Expected 22 ByteBuffer allocations for large file read with heap allocator");
        }

        @Test
        void offHeapAllocatorConfigured() throws IOException {
            var inputFile = S3InputFile.of(LARGE_FILE_URL);
            var allocator = new ByteBufferAllocatorWrapper(DirectByteBufferAllocator.getInstance());
            List<ProjectionRecord> actual = new CarpetReader.Builder<>(inputFile, ProjectionRecord.class)
                    .withAllocator(allocator)
                    .build()
                    .toList();
            assertEquals(RECORD_COUNT, actual.size());
            assertEquals(firstRecord.id, actual.get(0).id);
            assertEquals(firstRecord.payload, actual.get(0).payload);
            assertEquals(lastRecord.id, actual.get(RECORD_COUNT - 1).id);
            assertEquals(lastRecord.payload, actual.get(RECORD_COUNT - 1).payload);
            // Off-heap allocator can not use vectorized reads, and fall backs to per page
            // reads, resulting in more allocations. The exact number may vary based on
            // parquet file structure and reader implementation details, but should be
            // significantly higher than the heap allocator case.
            assertEquals(63, allocator.allocationCount(),
                    "Expected 63 ByteBuffer allocations for large file read with off-heap allocator");
        }

        private static class ByteBufferAllocatorWrapper implements ByteBufferAllocator {

            private final ByteBufferAllocator delegate;
            private long allocations = 0;

            public ByteBufferAllocatorWrapper(ByteBufferAllocator delegate) {
                this.delegate = delegate;
            }

            @Override
            public ByteBuffer allocate(int size) {
                allocations++;
                return delegate.allocate(size);
            }

            @Override
            public void release(ByteBuffer b) {
                delegate.release(b);
            }

            @Override
            public boolean isDirect() {
                return delegate.isDirect();
            }

            public long allocationCount() {
                return allocations;
            }

        }
    }

}
