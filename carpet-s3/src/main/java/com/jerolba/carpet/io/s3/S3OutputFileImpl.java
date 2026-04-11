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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

class S3OutputFileImpl implements S3OutputFile {

    private static final Logger logger = LoggerFactory.getLogger(S3OutputFileImpl.class);

    static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    private final S3Client client;
    private final String bucket;
    private final String key;
    private final Executor executor;

    S3OutputFileImpl(S3Client client, String bucket, String key, Executor executor) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.executor = executor;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        if (objectExists()) {
            throw new IOException("S3 object already exists: s3://" + bucket + "/" + key);
        }
        return createOrOverwrite(blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return new S3MultipartPositionOutputStream(bucket, key, client, executor);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @Override
    public String getPath() {
        return "s3://" + bucket + "/" + key;
    }

    private boolean objectExists() throws IOException {
        try {
            client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            throw new IOException("Failed to check if S3 object exists: s3://" + bucket + "/" + key, e);
        }
    }

    private static class S3MultipartPositionOutputStream extends PositionOutputStream {

        private final String bucket;
        private final String key;
        private final S3Client client;
        private final Executor executor;
        private final String uploadId;
        private final List<CompletableFuture<CompletedPart>> futures = new ArrayList<>();
        private ByteArrayOutputStream partBuffer = new ByteArrayOutputStream(MIN_PART_SIZE);
        private long pos = 0;
        private boolean closed = false;

        S3MultipartPositionOutputStream(String bucket, String key, S3Client client, Executor executor)
                throws IOException {
            this.bucket = bucket;
            this.key = key;
            this.client = client;
            this.executor = executor;
            try {
                this.uploadId = client.createMultipartUpload(
                        CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build())
                        .uploadId();
            } catch (Exception e) {
                throw new IOException("Failed to start S3 multipart upload: s3://" + bucket + "/" + key, e);
            }
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            ensureOpen();
            partBuffer.write(b);
            pos++;
            flushPartIfNeeded();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            ensureOpen();
            partBuffer.write(b, off, len);
            pos += len;
            flushPartIfNeeded();
        }

        @Override
        public void write(byte[] b) throws IOException {
            ensureOpen();
            write(b, 0, b.length);
        }

        @Override
        public void flush() throws IOException {
            ensureOpen();
            // buffered in memory; actual flush happens at part boundaries.
            // Multipart upload does not support uploading partial parts, so we only flush
            // when a full part is ready or on close.
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                flushFinalPart();
                completeUpload(collectCompletedParts());
                futures.clear();
            } catch (Exception e) {
                abortUpload();
                throw new IOException("Failed to complete S3 multipart upload: s3://" + bucket + "/" + key, e);
            } finally {
                partBuffer = null;
                if (executor instanceof CustomExecutor customExecutor) {
                    customExecutor.shutdown();
                }
            }
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
        }

        private void flushPartIfNeeded() {
            if (partBuffer.size() >= MIN_PART_SIZE) {
                uploadPart();
            }
        }

        private void flushFinalPart() {
            if (partBuffer.size() > 0) {
                uploadPart();
            }
        }

        private void uploadPart() {
            int partNumber = futures.size() + 1;
            byte[] data = partBuffer.toByteArray();
            partBuffer.reset();
            long partStartPos = pos - data.length;
            CompletableFuture<CompletedPart> future = CompletableFuture.supplyAsync(
                    () -> uploadPartS3(partNumber, partStartPos, data), executor);
            futures.add(future);
        }

        private CompletedPart uploadPartS3(int partNumber, long startPos, byte[] data) {
            logger.debug("Uploading part {} to s3://{}/{}: position={}, length={}",
                    partNumber, bucket, key, startPos, data.length);
            long startTime = System.nanoTime();
            UploadPartResponse response = client.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .contentLength((long) data.length)
                            .build(),
                    RequestBody.fromBytes(data));
            long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
            logger.debug("Uploaded part {} to s3://{}/{}: position={}, length={} bytes in {} ms",
                    partNumber, bucket, key, startPos, data.length, elapsedMs);
            return CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(response.eTag())
                    .build();
        }

        /**
         * Waits for all submitted part uploads to complete and collects the results in
         * part-number order. This is the guarantee that all parts have been uploaded
         * before {@link #completeUpload} is called.
         */
        private List<CompletedPart> collectCompletedParts() throws IOException {
            long count = futures.stream().filter(future -> !future.isDone()).count();
            logger.debug("Waiting for {} part uploads to complete for s3://{}/{}", count, bucket, key);
            List<CompletedPart> parts = new ArrayList<>(futures.size());
            for (CompletableFuture<CompletedPart> future : futures) {
                try {
                    parts.add(future.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for part upload to s3://"
                            + bucket + "/" + key, e);
                } catch (ExecutionException e) {
                    throw new IOException("Part upload failed for s3://" + bucket + "/" + key, e.getCause());
                }
            }
            return parts;
        }

        private void completeUpload(List<CompletedPart> parts) {
            client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                    .build());
        }

        private void abortUpload() {
            try {
                client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .uploadId(uploadId)
                        .build());
            } catch (Exception ignored) {
            }
        }
    }

}
