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

import static com.jerolba.carpet.io.s3.CustomExecutor.createCustomExecutor;
import static com.jerolba.carpet.io.s3.CustomExecutor.createVirtualThreadExecutorWithCommonPoolFallback;
import static com.jerolba.carpet.io.s3.S3UrlParsing.detectLocalFilePathForReading;
import static com.jerolba.carpet.io.s3.S3UrlParsing.parses3Url;

import java.nio.file.Path;
import java.util.concurrent.Executor;

import org.apache.parquet.io.InputFile;

import com.jerolba.carpet.io.s3.S3UrlParsing.S3Path;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * An extension of Parquet's {@link InputFile} interface that reads from an S3
 * object.The builder pattern is used to allow flexible configuration of the S3
 * client, bucket, key, and concurrency settings.
 *
 * If no Client is provided, a default S3Client will be created using the
 * default AWS credentials provider chain and region provider chain.
 *
 * For large files data is read using parallel ranged requests to S3, with the
 * concurrency level configurable via the {@link Builder#concurrency(int)}
 * method or by providing a custom {@link Executor} via the
 * {@link Builder#executor(Executor)} method. By default, a virtual thread
 * executor with a common pool fallback is used to allow for efficient
 * concurrent reads without blocking platform threads.
 *
 * If the S3 path provided to the builder is detected as a local file path (i.e.
 * it does not start with s3:// or s3a:// and points to an existing file), the
 * builder will automatically create a {@link LocalInputFile} instance that can
 * be used to read from the local file system instead of S3. This allows for
 * seamless testing and development with local files without changing the code
 * that uses the builder.
 *
 * If system property {@code carpet.s3.predownload.file} is set to true, the
 * entire S3 file will be pre-downloaded to a local temporary file before
 * reading. This can improve performance for small files or when the S3 object
 * is accessed multiple times in development, but it may not be suitable for
 * large files or production use due to of local disk usage.
 *
 */
public interface S3InputFile extends InputFile {

    /**
     * Configuration property to enable pre-downloading the entire S3 file to a
     * local temporary file before reading. This can improve performance for small
     * files or when the S3 object is accessed multiple times in development.
     */
    public static final String CARPET_S3_PREDOWNLOAD_FILE = "carpet.s3.predownload.file";

    public static S3InputFile of(String s3Path) {
        return new Builder().s3Path(s3Path).build();
    }

    /**
     * Returns a new builder for {@link S3InputFile}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a new builder for {@link S3InputFile}.
     *
     * @param s3Url the S3 URL in the format s3://bucket/key or s3a://bucket/key
     *
     * @return a new builder
     */
    public static Builder builder(String s3Url) {
        return new Builder().s3Path(s3Url);
    }

    /**
     * Returns a new builder for {@link S3InputFile} with the specified bucket and
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
         * Default concurrency level for vectored read operations when using the virtual
         * thread executor.
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
         * region provider chain. Providing a custom client allows for advanced
         * configurations such as custom retry policies, timeouts, or using a specific
         * AWS region.
         *
         * @param client the S3 client to use for operations
         * @return this builder
         */
        public Builder s3Client(S3Client client) {
            this.client = client;
            return this;
        }

        /**
         * Configures the S3 bucket name to read from.
         *
         * @param bucket the S3 bucket name
         * @return this builder
         */
        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        /**
         * Configures the S3 object key to read from.
         *
         * @param key the S3 object key
         * @return this builder
         */
        public Builder key(String key) {
            this.key = key;
            return this;
        }

        /**
         * Configures the S3 bucket and key by parsing a single S3 path string. The path
         * should be in the format s3://bucket/key or s3a://bucket/key. This is a
         * convenient method for setting both the bucket and key in one step, and it
         * includes validation to ensure the path is well-formed.
         *
         * @param s3Path the S3 path string to parse for bucket and key
         * @return this builder
         */
        public Builder s3Path(String s3Path) {
            Path local = detectLocalFilePathForReading(s3Path);
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
         * Configures the concurrency level for vectored read operations. A value of 1
         * means sequential reads, while values greater than 1 enable concurrent reads.
         * Mutually exclusive with {@link #executor(Executor)}.
         *
         * @param concurrency the concurrency level for vectored read operations, must
         *                    be > 0
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
         * Sets a custom executor for vectored read operations. Mutually exclusive with
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
         * Builds the {@link S3InputFile} instance based on the configured properties.
         *
         * @return the configured {@link S3InputFile} instance
         */
        public S3InputFile build() {
            if (localFilePath != null) {
                return new LocalInputFile(localFilePath);
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
            Executor executor = this.executor;
            if (concurrency != null && concurrency > 1) {
                executor = createCustomExecutor(concurrency);
            } else if (concurrency != null && concurrency == 1) {
                executor = null; // Use sequential reads
            } else {
                executor = createVirtualThreadExecutorWithCommonPoolFallback(DEFAULT_CONCURRENCY);
            }
            return new S3InputFileImpl(actualClient, bucket, key, executor);
        }

    }

}