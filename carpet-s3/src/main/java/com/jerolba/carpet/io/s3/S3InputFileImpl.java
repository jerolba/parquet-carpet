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
import java.nio.file.Path;
import java.util.concurrent.Executor;

import org.apache.parquet.io.SeekableInputStream;

import software.amazon.awssdk.services.s3.S3Client;

class S3InputFileImpl implements S3InputFile {

    private final S3SeekableReader s3SeekableReader;
    private final Executor executor;
    private final PrefetchConfig prefetchConfig;

    /**
     * Constructs an S3InputFile with the specified S3 client, s3 bucket and key,
     * executor and prefetch configuration.
     *
     * @param client         the S3 client to use for operations
     * @param bucket         the S3 bucket name
     * @param key            the S3 object key
     * @param executor       the executor to use for vectored read operations, or
     *                       null
     * @param prefetchConfig speculative prefetch configuration
     */
    S3InputFileImpl(S3Client client, String bucket, String key, Executor executor, PrefetchConfig prefetchConfig) {
        this.s3SeekableReader = new S3SeekableReader(client, bucket, key);
        this.executor = executor;
        this.prefetchConfig = prefetchConfig;
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

    @Override
    public String toString() {
        return s3SeekableReader.toString();
    }

    /**
     * Creates a new stream for reading from the S3 object.
     *
     * @return a new SeekableInputStream for reading from the S3 object
     * @throws IOException if an error occurs while creating the stream
     */
    @Override
    public SeekableInputStream newStream() throws IOException {
        if (Boolean.getBoolean(CARPET_S3_PREDOWNLOAD_FILE)) {
            Path localFile = s3SeekableReader.downloadToLocalFile();
            return new LocalFileInputStream(localFile);
        }
        CachedFooterProxyReader cachedReader = new CachedFooterProxyReader(s3SeekableReader);
        RangeReader rangeReader = prefetchConfig.create(cachedReader, executor);
        return new SeekableCachedFooterReader(cachedReader, rangeReader);
    }

    /**
     * The resolved prefetch configuration, exposed for tests.
     */
    PrefetchConfig prefetchConfig() {
        return prefetchConfig;
    }

}
