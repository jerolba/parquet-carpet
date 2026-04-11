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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class S3UrlParsingTest {

    @Test
    void parseS3Url() {
        S3UrlParsing.S3Path s3Path = S3UrlParsing.parses3Url("s3://bucket/key");
        assertEquals("bucket", s3Path.bucket());
        assertEquals("key", s3Path.key());

        s3Path = S3UrlParsing.parses3Url("s3a://bucket/key/with/slashes");
        assertEquals("bucket", s3Path.bucket());
        assertEquals("key/with/slashes", s3Path.key());

        s3Path = S3UrlParsing.parses3Url("s3://bucket-123.region/complex/key-_123");
        assertEquals("bucket-123.region", s3Path.bucket());
        assertEquals("complex/key-_123", s3Path.key());
    }

    @Test
    void parseS3UrlInvalid() {
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url(null));
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url(""));
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url("http://bucket/key"));
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url("s3://bucket"));
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url("s3://bucket/"));
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url("s3a://bucket/"));
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url("s3://"));
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url("s3a://"));
        assertThrows(IllegalArgumentException.class, () -> S3UrlParsing.parses3Url("s3:/bucket/key"));
    }

    @Test
    void detectLocalFilePathForReading(@TempDir Path tempDir) throws IOException {
        Path existingFile = tempDir.resolve("test.txt");
        Files.writeString(existingFile, "content");
        Path nonExistingFile = tempDir.resolve("non-existing.txt");
        Path existingDir = tempDir.resolve("subdir");
        Files.createDirectory(existingDir);

        assertEquals(existingFile, S3UrlParsing.detectLocalFilePathForReading(existingFile.toString()));
        assertEquals(existingDir, S3UrlParsing.detectLocalFilePathForReading(existingDir.toString()));
        assertNull(S3UrlParsing.detectLocalFilePathForReading(nonExistingFile.toString()));
        assertNull(S3UrlParsing.detectLocalFilePathForReading("s3://bucket/key"));
        assertNull(S3UrlParsing.detectLocalFilePathForReading("s3a://bucket/key"));
        assertNull(S3UrlParsing.detectLocalFilePathForReading(null));
        assertNull(S3UrlParsing.detectLocalFilePathForReading(""));
    }

    @Test
    void detectLocalFilePathForReadingWithFileUri(@TempDir Path tempDir) throws IOException {
        Path existingFile = tempDir.resolve("test.txt");
        Files.writeString(existingFile, "content");

        String fileUri = existingFile.toUri().toString();
        assertEquals(existingFile, S3UrlParsing.detectLocalFilePathForReading(fileUri));

        assertNull(S3UrlParsing.detectLocalFilePathForReading("file:///non-existing-path-xyz-123"));
    }

    @Test
    void detectLocalFilePathForWriting(@TempDir Path tempDir) {
        Path fileInExistingDir = tempDir.resolve("newfile.txt");
        Path fileInNonExistingDir = tempDir.resolve("non-existing-dir/newfile.txt");

        assertEquals(fileInExistingDir, S3UrlParsing.detectLocalFilePathForWriting(fileInExistingDir.toString()));
        assertNull(S3UrlParsing.detectLocalFilePathForWriting(fileInNonExistingDir.toString()));
        assertNull(S3UrlParsing.detectLocalFilePathForWriting("s3://bucket/key"));
        assertNull(S3UrlParsing.detectLocalFilePathForWriting("s3a://bucket/key"));
        assertNull(S3UrlParsing.detectLocalFilePathForWriting(null));
        assertNull(S3UrlParsing.detectLocalFilePathForWriting(""));
    }

    @Test
    void detectLocalFilePathForWritingWithFileUri(@TempDir Path tempDir) {
        Path fileInExistingDir = tempDir.resolve("newfile.txt");
        String fileUri = fileInExistingDir.toUri().toString();
        assertEquals(fileInExistingDir, S3UrlParsing.detectLocalFilePathForWriting(fileUri));

        Path fileInNonExistingDir = tempDir.resolve("subdir/newfile.txt");
        String fileUriInNonExistingDir = fileInNonExistingDir.toUri().toString();
        assertNull(S3UrlParsing.detectLocalFilePathForWriting(fileUriInNonExistingDir));
    }
}
