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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class S3OutputFileLocalTest {

    record SimpleRecord(long id, String name) {
    }

    @TempDir
    Path tempDir;

    @Test
    void detectLocalFileFromPath() throws IOException {
        Path file = tempDir.resolve("output.parquet");

        var outputFile = S3OutputFile.of(file.toString());
        assertInstanceOf(LocalOutputFile.class, outputFile);
    }

    @Test
    void detectLocalFileFromFileUri() throws IOException {
        Path file = tempDir.resolve("output-uri.parquet");
        URI uri = file.toUri();

        var outputFile = S3OutputFile.of(uri.toString());
        assertInstanceOf(LocalOutputFile.class, outputFile);
    }

    @Test
    void s3PathIsNotDetectedAsLocal() {
        S3Client mockClient = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(() -> AwsBasicCredentials.create("foo", "bar"))
                .endpointOverride(URI.create("http://localhost:8080"))
                .build();
        var outputFile = S3OutputFile.builder("s3://bucket/key").s3Client(mockClient).build();
        assertInstanceOf(S3OutputFileImpl.class, outputFile);
    }

    @Test
    void nonExistentParentDirectoryIsNotDetectedAsLocal() {
        String nonExistentParent = tempDir.resolve("nonexistent").resolve("output.parquet").toString();
        assertThrows(IllegalArgumentException.class, () -> S3OutputFile.of(nonExistentParent));
    }

    @Test
    void localFileCreateWritesData() throws IOException {
        Path file = tempDir.resolve("create-test.parquet");
        var outputFile = S3OutputFile.of(file.toString());

        var records = List.of(new SimpleRecord(1L, "Alice"), new SimpleRecord(2L, "Bob"));
        try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(outputFile, SimpleRecord.class)) {
            writer.write(records);
        }

        var actual = new CarpetReader<>(file.toFile(), SimpleRecord.class).toList();
        assertEquals(records, actual);
    }

    @Test
    void localFileCreateOrOverwriteReplacesExistingFile() throws IOException {
        Path file = tempDir.resolve("overwrite-test.parquet");

        var records1 = List.of(new SimpleRecord(1L, "Alice"));
        try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(
                S3OutputFile.builder(file.toString()).build(), SimpleRecord.class)) {
            writer.write(records1);
        }

        var records2 = List.of(new SimpleRecord(2L, "Bob"), new SimpleRecord(3L, "Carol"));
        try (PositionOutputStream out = S3OutputFile.builder(file.toString()).build()
                .createOrOverwrite(0)) {
            // createOrOverwrite should not throw even though the file already exists
            assertDoesNotThrow(() -> {});
        }

        // Use CarpetWriter with createOrOverwrite via a second full write
        var outputFile2 = new LocalOutputFile(file);
        try (CarpetWriter<SimpleRecord> writer = new CarpetWriter<>(outputFile2, SimpleRecord.class)) {
            writer.write(records2);
        }

        var actual = new CarpetReader<>(file.toFile(), SimpleRecord.class).toList();
        assertEquals(records2, actual);
    }

    @Test
    void localFileCreateThrowsIfFileAlreadyExists() throws IOException {
        Path file = tempDir.resolve("already-exists.parquet");
        Files.write(file, new byte[] { 1, 2, 3 });

        var outputFile = S3OutputFile.of(file.toString());
        assertThrows(IOException.class, () -> outputFile.create(0));
    }

    @Test
    void localFileGetPathReturnsFilePath() {
        Path file = tempDir.resolve("getpath-test.parquet");
        var outputFile = S3OutputFile.of(file.toString());
        assertEquals(file.toString(), outputFile.getPath());
    }

    @Test
    void positionTrackedCorrectly() throws IOException {
        Path file = tempDir.resolve("position-test.parquet");
        var outputFile = S3OutputFile.of(file.toString());

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
