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

import static com.jerolba.carpet.io.s3.CustomExecutor.createCustomExecutor;
import static com.jerolba.carpet.io.s3.CustomExecutor.createVirtualThreadExecutorWithCommonPoolFallback;
import static com.jerolba.carpet.io.s3.S3UrlParsing.detectLocalFilePathForWriting;
import static com.jerolba.carpet.io.s3.S3UrlParsing.parses3Url;

import java.nio.file.Path;
import java.util.concurrent.Executor;

import org.apache.parquet.io.OutputFile;

import com.jerolba.carpet.io.s3.S3UrlParsing.S3Path;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * An extension of Parquet's {@link OutputFile} interface that writes to an S3
 * object. The builder pattern is used to allow flexible configuration of the S3
 * client, bucket, and key.
 *
 * If no Client is provided, a default S3Client will be created using the
 * default AWS credentials provider chain and region provider chain.
 *
 * Data is buffered in a local temporary file and uploaded to S3 when the output
 * stream is closed.
 *
 * If the path provided to the builder is detected as a local file path (i.e. it
 * does not start with s3:// or s3a:// and points to a valid writable location),
 * the builder will automatically create a {@link LocalOutputFile} instance that
 * writes to the local file system instead of S3. "Valid" means the path is
 * syntactically correct and its parent directory exists. This allows for
 * seamless testing and development with local files without changing the code
 * that uses the builder.
 */
public interface S3OutputFile extends OutputFile {

    public static S3OutputFile of(String s3Path) {
        return new Builder().s3Path(s3Path).build();
    }

    /**
     * Returns a new builder for {@link S3OutputFile}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a new builder for {@link S3OutputFile}.
     *
     * @param s3Url the S3 URL in the format s3://bucket/key or s3a://bucket/key
     *
     * @return a new builder
     */
    public static Builder builder(String s3Url) {
        return new Builder().s3Path(s3Url);
    }

    /**
     * Returns a new builder for {@link S3OutputFile} with the specified bucket and
     * key.
     *
     * @param bucket the S3 bucket name
     * @param key    the S3 object key
     * @return a new builder
     */
    public static Builder builder(String bucket, String key) {
        return new Builder().bucket(bucket).key(key);
    }

    public static class Builder {

        /**
         * Default concurrency level for part uploads when using the virtual thread
         * executor.
         */
        private static final int DEFAULT_CONCURRENCY = 4;

        private S3Client client;
        private String bucket;
        private String key;
        private Integer concurrency = null;
        private Executor executor = null;
        private Path localFilePath = null;

        /**
         * Configures the S3 client to use for operations. If not set, a default
         * S3Client will be created using the default AWS credentials provider chain and
         * region provider chain.
         *
         * @param client the S3 client to use for operations
         * @return this builder
         */
        public Builder s3Client(S3Client client) {
            this.client = client;
            return this;
        }

        /**
         * Configures the S3 bucket name to write to.
         *
         * @param bucket the S3 bucket name
         * @return this builder
         */
        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        /**
         * Configures the S3 object key to write to.
         *
         * @param key the S3 object key
         * @return this builder
         */
        public Builder key(String key) {
            this.key = key;
            return this;
        }

        /**
         * Configures the concurrency level for parallel part uploads. A value of 1
         * means sequential uploads, while values greater than 1 enable concurrent
         * uploads. Mutually exclusive with {@link #executor(Executor)}.
         *
         * @param concurrency the concurrency level, must be > 0
         * @return this builder
         */
        public Builder concurrency(int concurrency) {
            if (concurrency <= 0) {
                throw new IllegalArgumentException("concurrency must be > 0, got: " + concurrency);
            }
            this.concurrency = concurrency;
            return this;
        }

        /**
         * Sets a custom executor for parallel part uploads. Mutually exclusive with
         * {@link #concurrency(int)}.
         *
         * @param executor the executor to use
         * @return this builder
         */
        public Builder executor(Executor executor) {
            if (executor == null) {
                throw new IllegalArgumentException("executor must not be null");
            }
            this.executor = executor;
            return this;
        }

        /**
         * Configures the S3 bucket and key by parsing a single S3 path string. The path
         * should be in the format s3://bucket/key or s3a://bucket/key. This is a
         * convenient method for setting both the bucket and key in one step, and it
         * includes validation to ensure the path is well-formed.
         *
         * If the path is not an S3 URL but is a valid local file system path (its
         * parent directory exists), the builder will fall back to creating a
         * {@link LocalOutputFile}.
         *
         * @param s3Path the S3 path string to parse for bucket and key, or a local file
         *               system path
         * @return this builder
         */
        public Builder s3Path(String s3Path) {
            Path local = detectLocalFilePathForWriting(s3Path);
            if (local != null) {
                this.localFilePath = local;
            } else {
                S3Path s3PathObj = parses3Url(s3Path);
                this.bucket = s3PathObj.bucket();
                this.key = s3PathObj.key();
            }
            return this;
        }

        /**
         * Builds the {@link S3OutputFile} instance based on the configured properties.
         *
         * @return the configured {@link S3OutputFile} instance
         */
        public S3OutputFile build() {
            if (localFilePath != null) {
                return new LocalOutputFile(localFilePath);
            }
            if (bucket == null || bucket.isEmpty()) {
                throw new IllegalStateException("bucket must be configured");
            }
            if (key == null || key.isEmpty()) {
                throw new IllegalStateException("key must be configured");
            }
            if (concurrency != null && executor != null) {
                throw new IllegalStateException("concurrency and executor are mutually exclusive");
            }
            S3Client actualClient = this.client;
            if (actualClient == null) {
                actualClient = S3Client.create();
            }
            Executor actualExecutor = this.executor;
            if (concurrency != null && concurrency > 1) {
                actualExecutor = createCustomExecutor(concurrency);
            } else if (concurrency != null && concurrency == 1) {
                actualExecutor = Runnable::run; // sequential: run in calling thread
            } else {
                actualExecutor = createVirtualThreadExecutorWithCommonPoolFallback(DEFAULT_CONCURRENCY);
            }
            return new S3OutputFileImpl(actualClient, bucket, key, actualExecutor);
        }

    }

}
