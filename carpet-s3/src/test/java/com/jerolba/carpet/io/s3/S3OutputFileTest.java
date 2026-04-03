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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

class S3OutputFileTest {

    private static final String BUCKET_NAME = "test-bucket";

    private static LocalStackContainer localStack;

    @BeforeAll
    static void setUp() {
        localStack = new LocalStackContainer(DockerImageName.parse(
                "localstack/localstack:s3-community-archive:b14111811a1071ff8e05ea2d89fac68dc3aa115bcb0b053f5502a1dfffba4ff8"))
                        .withServices("s3");
        localStack.start();
        System.setProperty("aws.endpointUrl", localStack.getEndpoint().toString());
        System.setProperty("aws.accessKeyId", localStack.getAccessKey());
        System.setProperty("aws.secretAccessKey", localStack.getSecretKey());
        System.setProperty("aws.region", localStack.getRegion());

        try (S3Client s3Client = S3Client.create()) {
            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(BUCKET_NAME)
                    .build());
        }
    }

    @AfterAll
    static void tearDown() {
        if (localStack != null) {
            localStack.stop();
        }
        System.clearProperty("aws.endpointUrl");
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    record SimpleRecord(long id, String name, double value) {
    }

    record WideRecord(long id, String name, double value, int count, String description) {
    }

    record NarrowRecord(long id, String name) {
    }

    @Nested
    class WriteAndRead {

        @Test
        void writeAndReadSimpleRecords() throws IOException {
            var expected = List.of(
                    new SimpleRecord(1, "Alice", 10.5),
                    new SimpleRecord(2, "Bob", 20.3));
            String url = "s3://test-bucket/write-read-simple.parquet";

            try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(S3OutputFile.of(url), SimpleRecord.class)) {
                writer.write(expected);
            }

            List<SimpleRecord> actual = new CarpetReader<>(S3InputFile.of(url), SimpleRecord.class).toList();
            assertEquals(expected, actual);
        }

        @Test
        void writeAndReadMultipleRecords() throws IOException {
            var expected = IntStream.range(0, 100)
                    .mapToObj(i -> new SimpleRecord(i, "name-" + i, i * 1.1))
                    .toList();
            String url = "s3://test-bucket/write-read-multiple.parquet";

            try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(S3OutputFile.of(url), SimpleRecord.class)) {
                writer.write(expected);
            }

            List<SimpleRecord> actual = new CarpetReader<>(S3InputFile.of(url), SimpleRecord.class).toList();
            assertEquals(expected, actual);
        }

        @Test
        void writeAndReadEmptyFile() throws IOException {
            String url = "s3://test-bucket/write-read-empty.parquet";

            try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(S3OutputFile.of(url), SimpleRecord.class)) {
                writer.write(List.of());
            }

            List<SimpleRecord> actual = new CarpetReader<>(S3InputFile.of(url), SimpleRecord.class).toList();
            assertTrue(actual.isEmpty());
        }

        @Test
        void writeAndReadWithProjection() throws IOException {
            var records = List.of(
                    new WideRecord(1, "Alice", 10.5, 42, "First"),
                    new WideRecord(2, "Bob", 20.3, 99, "Second"));
            String url = "s3://test-bucket/write-read-projection.parquet";

            try (CarpetWriter<WideRecord> writer = new CarpetWriter<>(S3OutputFile.of(url), WideRecord.class)) {
                writer.write(records);
            }

            List<NarrowRecord> actual = new CarpetReader<>(S3InputFile.of(url), NarrowRecord.class).toList();
            assertEquals(2, actual.size());
            assertEquals(new NarrowRecord(1, "Alice"), actual.get(0));
            assertEquals(new NarrowRecord(2, "Bob"), actual.get(1));
        }
    }

    @Nested
    class GetPath {

        @Test
        void getPathReturnS3Url() {
            try (S3Client s3Client = S3Client.create()) {
                var outputFile = S3OutputFile.builder("s3://test-bucket/get-path.parquet")
                        .s3Client(s3Client).build();
                assertEquals("s3://test-bucket/get-path.parquet", outputFile.getPath());
            }
        }
    }

    @Nested
    class CreateBehavior {

        @Test
        void createThrowsIfObjectAlreadyExists() throws IOException {
            String url = "s3://test-bucket/create-already-exists.parquet";

            try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(S3OutputFile.of(url), SimpleRecord.class)) {
                writer.write(List.of(new SimpleRecord(1, "Alice", 1.0)));
            }

            var outputFile = S3OutputFile.of(url);
            assertThrows(IOException.class, () -> outputFile.create(0));
        }

        @Test
        void createOrOverwriteReplacesExistingObject() throws IOException {
            String url = "s3://test-bucket/create-or-overwrite.parquet";
            var first = List.of(new SimpleRecord(1, "Alice", 1.0));
            var second = List.of(new SimpleRecord(2, "Bob", 2.0), new SimpleRecord(3, "Carol", 3.0));

            try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(S3OutputFile.of(url), SimpleRecord.class)) {
                writer.write(first);
            }

            // CarpetWriter uses OVERWRITE mode, which calls createOrOverwrite
            try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(S3OutputFile.of(url), SimpleRecord.class)) {
                writer.write(second);
            }

            List<SimpleRecord> actual = new CarpetReader<>(S3InputFile.of(url), SimpleRecord.class).toList();
            assertEquals(second, actual);
        }
    }

    @Nested
    class ErrorCases {

        @Test
        void writeToNonExistentBucketThrows() throws IOException {
            var outputFile = S3OutputFile.builder("s3://no-such-bucket/some-key.parquet").build();
            assertThrows(Exception.class, () -> {
                try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(outputFile, SimpleRecord.class)) {
                    writer.write(List.of(new SimpleRecord(1, "Alice", 1.0)));
                }
            });
        }
    }

    @Nested
    class PositionTracking {

        @Test
        void positionIsTrackedCorrectly() throws IOException {
            String url = "s3://test-bucket/position-tracking.parquet";
            var outputFile = S3OutputFile.of(url);

            try (PositionOutputStream out = outputFile.createOrOverwrite(0)) {
                assertEquals(0, out.getPos());
                out.write(new byte[100]);
                assertEquals(100, out.getPos());
                out.write(new byte[50], 0, 50);
                assertEquals(150, out.getPos());
                out.write(42);
                assertEquals(151, out.getPos());
            }
        }
    }

    @Nested
    class LargeFileScenarios {

        /**
         * Builds a 512-char incompressible string using a 32-bit LCG (same as
         * S3InputFileTest) to keep the parquet file large enough to span multiple
         * multipart upload parts (> 5 MiB each).
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

        record HeavyRecord(long id, long value, String payload) {
        }

        @Test
        void writeLargeFileSpanningMultipleParts() throws IOException {
            int recordCount = 40_000;
            String url = "s3://test-bucket/write-large.parquet";

            var expected = IntStream.range(0, recordCount)
                    .mapToObj(i -> new HeavyRecord(i, i * 2L, makePayload(i)))
                    .toList();

            try (CarpetWriter<HeavyRecord> writer = new CarpetWriter<>(S3OutputFile.of(url), HeavyRecord.class)) {
                writer.write(expected);
            }

            // Verify the object exists in S3 and has realistic size
            try (S3Client s3Client = S3Client.create()) {
                var head = s3Client.headObject(HeadObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key("write-large.parquet")
                        .build());
                assertTrue(head.contentLength() > S3OutputFileImpl.MIN_PART_SIZE,
                        "Expected file larger than one multipart part, was: " + head.contentLength());
            }

            List<HeavyRecord> actual = new CarpetReader<>(S3InputFile.of(url), HeavyRecord.class).toList();
            assertEquals(recordCount, actual.size());
            assertEquals(expected.get(0), actual.get(0));
            assertEquals(expected.get(recordCount - 1), actual.get(recordCount - 1));
            assertEquals(expected, actual);
        }
    }

    @Nested
    class AbortOnError {

        @Test
        void abortIsCalledWhenUploadFails() {
            try (S3Client s3Client = S3Client.create()) {
                // Force failure after multipart upload has been created but before any part can
                // be completed, so close() must abort the multipart upload.
                var outputFile = new S3OutputFileImpl(s3Client, BUCKET_NAME, "abort-test.parquet",
                        runnable -> {
                            throw new java.util.concurrent.RejectedExecutionException("forced test failure");
                        });
                assertThrows(Exception.class, () -> {
                    try (PositionOutputStream out = outputFile.createOrOverwrite(0)) {
                        // write enough to trigger a part upload (> 5 MiB)
                        out.write(new byte[S3OutputFileImpl.MIN_PART_SIZE + 1]);
                    }
                });
                // Verify the key was not left behind (upload was aborted)
                try (S3Client goodClient = S3Client.create()) {
                    assertThrows(NoSuchKeyException.class, () -> goodClient.headObject(
                            HeadObjectRequest.builder()
                                    .bucket(BUCKET_NAME)
                                    .key("abort-test.parquet")
                                    .build()));
                }
            }
        }
    }
}
