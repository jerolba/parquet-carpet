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

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import org.apache.parquet.io.SeekableInputStream;

import software.amazon.awssdk.services.s3.S3Client;

class S3InputFileImpl implements S3InputFile {

    private final S3SeekableReader s3SeekableReader;
    private final Executor executor;

    /**
     * Constructs an S3InputFile with the specified S3 client, s3 bucket and key,
     * and executor.
     *
     * @param client   the S3 client to use for operations
     * @param bucket   the S3 bucket name
     * @param key      the S3 object key
     * @param executor the executor to use for vectored read operations, or null
     */
    S3InputFileImpl(S3Client client, String bucket, String key, Executor executor) {
        this.s3SeekableReader = new S3SeekableReader(client, bucket, key);
        this.executor = executor;
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

}