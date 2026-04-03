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
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.apache.parquet.bytes.ByteBufferAllocator;

/**
 * A {@link RangeReader} that reads each span sequentially in the calling thread.
 * <p>
 * Suitable for low-concurrency scenarios or when an executor is not available.
 * Each span is read as a single blocking call to the underlying
 * {@link SeekableReader}.
 * </p>
 */
class SequentialReader extends RangeReader {

    private final SeekableReader seekableReader;

    SequentialReader(SeekableReader seekableReader) {
        this.seekableReader = seekableReader;
    }

    @Override
    protected CompletableFuture<ByteBuffer> readSpan(ByteBufferAllocator allocator, long offset, int length)
            throws IOException {
        ByteBuffer buffer = allocator.allocate(length);
        try {
            seekableReader.readFully(offset, buffer);
            buffer.flip();
            return CompletableFuture.completedFuture(buffer);
        } catch (IOException e) {
            allocator.release(buffer);
            throw e;
        }
    }

    @Override
    public void close() {
    }

}
