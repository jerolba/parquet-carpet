package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import software.amazon.awssdk.services.s3.S3Client;

public class S3InputFile implements InputFile {

    /**
     * Configuration property to enable pre-downloading the entire S3 file to a
     * local temporary file before reading. This can improve performance for small
     * files or when the S3 object is accessed multiple times in development.
     */
    public static final String CARPET_S3_PREDOWNLOAD_FILE = "carpet.s3.predownload.file";

    private final S3SeekableReader s3SeekableReader;
    private final Executor executor;

    public S3InputFile(String s3Path) {
        this.executor = ForkJoinPool.commonPool();
        S3Path bucketKey = parse(s3Path);
        this.s3SeekableReader = new S3SeekableReader(S3Client.create(), bucketKey.bucket(), bucketKey.key());
    }

    /**
     * Constructs an S3InputFile with the specified S3 client, file path,
     * concurrency and executor. The {@code executor} and {@code concurrency} are
     * mutually exclusive: when an executor is provided, concurrency is ignored.
     *
     * @param client   the S3 client to use for operations
     * @param bucket   the S3 bucket name
     * @param key      the S3 object key
     * @param executor the executor to use for vectored read operations, or null
     */
    private S3InputFile(S3Client client, String bucket, String key, Executor executor) {
        this.executor = executor;
        this.s3SeekableReader = new S3SeekableReader(client, bucket, key);
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
     * @param url the S3 URL in the format s3://bucket/key or s3a://bucket/key
     *
     * @return a new builder
     */
    public static Builder builder(String url) {
        return new Builder().s3Path(url);
    }

    /**
     * Returns the length of the S3 object.
     *
     * @return the length of the S3 object
     * @throws IOException if an error occurs while getting the length
     */
    @Override
    public long getLength() throws IOException {
        return s3SeekableReader.getLength();
    }

    /**
     * Creates a new stream for reading from the S3 object.
     *
     * @return a new SeekableInputStream for reading from the S3 object
     * @throws IOException if an error occurs while creating the stream
     */
    @Override
    public SeekableInputStream newStream() throws IOException {
        if ("true".equalsIgnoreCase(System.getProperty(CARPET_S3_PREDOWNLOAD_FILE))) {
            Path localFile = s3SeekableReader.downloadToLocalFile();
            return new LocalFileInputStream(localFile);
        }
        return new SeekableCachedFooterReader(s3SeekableReader, executor);
    }

    public static class Builder {

        private S3Client client;
        private String bucket;
        private String key;
        private Integer concurrency = null;
        private Executor executor = null;

        public Builder s3Client(S3Client client) {
            this.client = client;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder s3Path(String s3Path) {
            S3Path s3PathObj = parse(s3Path);
            this.bucket = s3PathObj.bucket();
            this.key = s3PathObj.key();
            return this;
        }

        /**
         * Sets the concurrency level for vectored read operations. A value of 1 means
         * sequential reads, while values greater than 1 enable concurrent reads.
         * Mutually exclusive with {@link #executor(Executor)}.
         *
         * @param concurrency
         * @return
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

        public S3InputFile build() {
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
                executor = new OwnedExecutor(concurrency);
            } else if (concurrency != null && concurrency == 1) {
                executor = null; // Use sequential reads
            } else {
                executor = ForkJoinPool.commonPool();
            }
            return new S3InputFile(actualClient, bucket, key, executor);
        }
    }

    private record S3Path(String bucket, String key) {
    }

    private static S3Path parse(String s3Path) {
        if (s3Path == null || s3Path.isEmpty()) {
            throw new IllegalArgumentException("s3Path cannot be null or empty");
        }
        String path = s3Path;
        if (path.startsWith("s3://")) {
            path = path.substring(5);
        } else if (path.startsWith("s3a://")) {
            path = path.substring(6);
        } else if (path.startsWith("/")) {
            path = path.substring(1);
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

}