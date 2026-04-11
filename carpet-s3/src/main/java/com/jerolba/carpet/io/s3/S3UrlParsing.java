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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

class S3UrlParsing {

    private static final String S3A = "s3a://";
    private static final String S3 = "s3://";

    public record S3Path(String bucket, String key) {
    }

    /**
     * Parses an S3 URL in the format s3://bucket/key or s3a://bucket/key and
     * returns an S3Path object containing the bucket and key.
     *
     * @param s3Path the S3 URL to parse
     * @return an S3Path object containing the bucket and key
     */
    public static S3Path parses3Url(String s3Path) {
        if (s3Path == null || s3Path.isEmpty()) {
            throw new IllegalArgumentException("s3Path cannot be null or empty");
        }
        String path = s3Path;
        if (path.startsWith(S3)) {
            path = path.substring(5);
        } else if (path.startsWith(S3A)) {
            path = path.substring(6);
        } else {
            throw new IllegalArgumentException("Invalid S3 path: " + s3Path + ".");
        }
        int slashIndex = path.indexOf('/');
        if (slashIndex == -1) {
            throw new IllegalArgumentException("Invalid S3 path: " + s3Path + ". Must be bucket/key");
        }
        String bucket = path.substring(0, slashIndex);
        String key = path.substring(slashIndex + 1);
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Invalid S3 path: " + s3Path + ". Key cannot be empty");
        }
        return new S3Path(bucket, key);
    }

    /**
     * Detects if the given path is a local file path that exists and can be read.
     * If the path is an S3 URL or is empty/null, this method returns null. If the
     * path is a valid local file path that exists, it returns the corresponding
     * Path object. If the path is a local file path that does not exist or cannot
     * be read, it returns null.
     *
     * This method is used as fallback logic in the builder methods to allow users
     * to specify either an S3 URL or a local file path when configuring input files
     * for testing without requiring an actual S3 environment.
     *
     * @param s3Path the path to check
     * @return the Path object if the path is a valid local file path, null
     *         otherwise
     */
    public static Path detectLocalFilePathForReading(String s3Path) {
        if (isS3OrEmpty(s3Path)) {
            return null;
        }
        try {
            Path path = parsePath(s3Path);
            return Files.exists(path) ? path : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Detects if the given path is a local file path that can be written to. If the
     * path is an S3 URL or is empty/null, this method returns null. If the path is
     * a valid local file path that can be written to (i.e. its parent directory
     * exists), it returns the corresponding Path object. If the path is a local
     * file path that cannot be written to (e.g. its parent directory does not
     * exist), it returns null.
     *
     * This method is used as fallback logic in the builder methods to allow users
     * to specify either an S3 URL or a local file path when configuring output
     * files for testing without requiring an actual S3 environment.
     *
     * @param path the path to check
     * @return the Path object if the path is a valid local file path that can be
     *         written to, null
     */
    public static Path detectLocalFilePathForWriting(String path) {
        if (isS3OrEmpty(path)) {
            return null;
        }
        try {
            Path filePath = parsePath(path);
            // Valid if no parent component (relative to current dir) or parent exists
            Path parent = filePath.getParent();
            if (parent == null || Files.isDirectory(parent)) {
                return filePath;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isS3OrEmpty(String path) {
        return path == null || path.isEmpty() || path.startsWith(S3) || path.startsWith(S3A);
    }

    private static Path parsePath(String s3Path) throws URISyntaxException {
        Path path;
        if (s3Path.startsWith("file://")) {
            path = Path.of(new URI(s3Path));
        } else {
            path = Path.of(s3Path);
        }
        return path;
    }

}
