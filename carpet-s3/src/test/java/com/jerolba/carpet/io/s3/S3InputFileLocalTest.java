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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class S3InputFileLocalTest {

    @TempDir
    Path tempDir;

    @Test
    void detectLocalFileFromPath() throws IOException {
        Path file = tempDir.resolve("test.parquet");
        Files.write(file, new byte[] { 1, 2, 3 });

        var inputFile = S3InputFile.of(file.toString());
        assertInstanceOf(LocalInputFile.class, inputFile);
        assertEquals(3, inputFile.getLength());
    }

    @Test
    void detectLocalFileFromFileUri() throws IOException {
        Path file = tempDir.resolve("test-uri.parquet");
        Files.write(file, new byte[] { 1, 2, 3, 4 });
        // Path.of(new URI("file:///...")) is the standard way to handle file URIs.
        URI uri = file.toUri();

        var inputFile = S3InputFile.of(uri.toString());
        assertInstanceOf(LocalInputFile.class, inputFile);
        assertEquals(4, inputFile.getLength());
    }

    @Test
    void s3PathIsNotDetectedAsLocal() {
        // Use a mock client to avoid S3Client.create() trying to find credentials
        S3Client mockClient = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(() -> AwsBasicCredentials.create("foo", "bar"))
                .endpointOverride(URI.create("http://localhost:8080"))
                .build();
        var inputFile = S3InputFile.builder("s3://bucket/key").s3Client(mockClient).build();
        assertInstanceOf(S3InputFileImpl.class, inputFile);
    }

    @Test
    void nonExistentPathIsNotDetectedAsLocal() {
        String nonExistent = tempDir.resolve("non-existent.parquet").toString();
        // S3InputFile.of() calls S3InputFile.builder().s3Path().build()
        // s3Path() -> detectLocalFilePath() returns null -> parse() fails with
        // IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> S3InputFile.of(nonExistent));
    }
}
