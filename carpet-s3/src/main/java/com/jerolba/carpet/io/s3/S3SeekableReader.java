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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * SeekableReader implementation that reads directly from S3 using ranged GET
 * requests.
 */
class S3SeekableReader implements SeekableReader {

    private static final Logger logger = LoggerFactory.getLogger(S3SeekableReader.class);

    private final S3Client client;
    private final String bucket;
    private final String key;
    private Long fileLength;
    private String etag;

    public S3SeekableReader(S3Client client, String bucket, String key) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public long getLength() throws IOException {
        if (fileLength == null) {
            try {
                HeadObjectRequest headRequest = HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();
                HeadObjectResponse headResponse = client.headObject(headRequest);
                fileLength = headResponse.contentLength();
                etag = headResponse.eTag().replaceAll("\"", ""); // Remove quotes from ETag if present
            } catch (Exception e) {
                throw new IOException("Failed to get length of S3 object: " + bucket + "/" + key, e);
            }
        }
        return fileLength;
    }

    @Override
    public int readFully(long pos, byte[] b, int off, int len) throws IOException {
        byte[] data = fetchRangeFromS3(pos, len);
        System.arraycopy(data, 0, b, off, len);
        return len;
    }

    @Override
    public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
        int len = byteBuffer.remaining();
        byte[] data = fetchRangeFromS3(pos, len);
        byteBuffer.put(data);
        return len;
    }

    private byte[] fetchRangeFromS3(long pos, int len) throws IOException {
        logger.debug("fetchRangeFromS3() - Reading: offset={}, length={}", pos, len);

        long init = System.nanoTime();
        GetObjectRequest getObjectRequest = createGetRequest(pos, len);
        byte[] data;
        try {
            ResponseBytes<GetObjectResponse> objectBytes = client.getObjectAsBytes(getObjectRequest);
            data = objectBytes.asByteArray();
            logger.debug("fetchRangeFromS3() - Finished: offset={}, length={}, speed={} MB/s",
                    pos, len, (len / (1024.0 * 1024.0)) / ((System.nanoTime() - init) / 1_000_000_000.0));
        } catch (Exception e) {
            throw new IOException("Failed to read from S3 object: " + bucket + "/" + key +
                    " at position " + pos + " for " + len + " bytes", e);
        }
        if (data.length < len) {
            throw new IOException("Expected " + len + " bytes but got " + data.length + " bytes from S3");
        }
        return data;
    }

    private GetObjectRequest createGetRequest(long pos, int len) {
        String range = String.format("bytes=%d-%d", pos, pos + len - 1);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range(range)
                .build();
        return getObjectRequest;
    }

    @Override
    public void close() throws IOException {
        // No resources to close for S3Client-based reads
        // The S3Client lifecycle is managed externally
    }

    /**
     * Downloads the S3 object to a local file in the system's temporary directory.
     * The file name is constructed using the S3 object key file name and ETag to
     * ensure uniqueness and avoid conflicts.
     *
     * @return the Path to the downloaded local file
     * @throws IOException if an error occurs while downloading the file
     */
    public Path downloadToLocalFile() throws IOException {
        getLength(); // Get etag and length
        String tmpDir = System.getProperty("java.io.tmpdir");
        String targetFileName = buildTargetFileName(key, etag);
        Path targetPath = Path.of(tmpDir, targetFileName);
        if (targetPath.toFile().exists() && Files.size(targetPath) == getLength()) {
            logger.info("Using cached local file for S3 object: {}", targetPath);
            return targetPath;
        }
        logger.info("Downloading S3 object to local file: s3://{}/{} -> {}", bucket, key, targetPath);
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            client.getObject(getObjectRequest, targetPath);
            return targetPath;
        } catch (Exception e) {
            throw new IOException("Failed to download S3 object: " + bucket + "/" + key, e);
        }
    }

    private String buildTargetFileName(String key, String etag) {
        int lastIndexOfSlash = key.lastIndexOf('/');
        String fileName = lastIndexOfSlash >= 0 ? key.substring(lastIndexOfSlash + 1) : key;
        int lastIndexOf = fileName.lastIndexOf('.');
        String prefix = lastIndexOf >= 0 ? fileName.substring(0, lastIndexOf) : fileName;
        String suffix = lastIndexOf >= 0 ? fileName.substring(lastIndexOf) : "";
        return prefix + "_" + etag + suffix;
    }

}