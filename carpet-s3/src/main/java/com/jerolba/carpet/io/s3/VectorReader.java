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

import static com.jerolba.carpet.io.s3.Cancellation.cancellation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.apache.parquet.bytes.ByteBufferAllocator;

/**
 * A {@link RangeReader} that reads spans asynchronously using a provided
 * {@link Executor}.
 * <p>
 * For spans smaller than {@code CHUNK_SIZE} (8 MB), the read is issued as a
 * single async task. For larger spans, the range is split into parallel chunks
 * of up to {@code CHUNK_SIZE} bytes, each read concurrently and assembled into
 * a single {@link java.nio.ByteBuffer}.
 * </p>
 * <p>
 * If the executor is an {@link CustomExecutor}, it will be shut down when this
 * reader is closed.
 * </p>
 */
class VectorReader extends RangeReader {

    private final SeekableReader seekableReader;
    private final Executor executor;
    private final ParallelReader parallelReader;

    VectorReader(SeekableReader seekableReader, Executor executor) {
        this.seekableReader = seekableReader;
        this.executor = executor;
        this.parallelReader = new ParallelReader(seekableReader, executor);
    }

    @Override
    protected CompletableFuture<ByteBuffer> readSpan(ByteBufferAllocator allocator, long startOffset, int totalLength,
            Cancellation cancellation) {
        if (totalLength > ParallelReader.CHUNK_SIZE) {
            return readSpanInParallelChunks(allocator, startOffset, totalLength, cancellation);
        }
        return readSpanSequentially(allocator, startOffset, totalLength, cancellation);
    }

    private CompletableFuture<ByteBuffer> readSpanSequentially(ByteBufferAllocator allocator,
            long startOffset, int totalLength, Cancellation cancellation) {
        return CompletableFuture.supplyAsync(() -> {
            cancellation.checkNotCancelled();
            ByteBuffer buffer = allocator.allocate(totalLength);
            try {
                seekableReader.readFully(startOffset, buffer);
                buffer.flip();
                return buffer;
            } catch (IOException e) {
                allocator.release(buffer);
                throw new CompletionException(e);
            }
        }, executor);
    }

    private CompletableFuture<ByteBuffer> readSpanInParallelChunks(ByteBufferAllocator allocator,
            long startOffset, int totalLength, Cancellation cancellation) {
        if (cancellation.isCancelled()) {
            return CompletableFuture.failedFuture(cancellation());
        }
        ByteBuffer buffer = allocator.allocate(totalLength);
        return parallelReader.readChunked(startOffset, buffer.slice(0, totalLength), cancellation)
                .thenApply(v -> {
                    buffer.position(0);
                    buffer.limit(totalLength);
                    return buffer;
                })
                .exceptionally(t -> {
                    allocator.release(buffer);
                    throw new CompletionException(t);
                });
    }

    @Override
    public void close() {
        if (executor instanceof CustomExecutor customExecutor) {
            customExecutor.shutdown();
        }
    }

}
