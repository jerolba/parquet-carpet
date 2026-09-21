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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Fills a caller-provided buffer with a section of the file, split into chunks
 * of up to {@code CHUNK_SIZE} bytes that are read concurrently on an
 * {@link Executor}.
 * <p>
 * It owns neither the buffer nor the executor: allocating and releasing the
 * buffer, and shutting down the executor, is up to the caller.
 * </p>
 */
class ParallelReader {

    static final int CHUNK_SIZE = 8 * 1024 * 1024; // 8 MB

    private final SeekableReader seekableReader;
    private final Executor executor;

    ParallelReader(SeekableReader seekableReader, Executor executor) {
        this.seekableReader = seekableReader;
        this.executor = executor;
    }

    /**
     * Reads {@code target.remaining()} bytes starting at {@code offset} into the
     * region of {@code target} between its position and its limit. The position
     * and limit of {@code target} are not modified: the reads go through slices.
     *
     * @param offset    file position of the first byte
     * @param target    destination buffer
     * @param cancelled flag checked before each chunk read starts
     * @return a future completed when all bytes have been written
     */
    CompletableFuture<Void> readChunked(long offset, ByteBuffer target, Cancellation cancelled) {
        ByteBuffer view = target.slice();
        int totalLength = view.remaining();
        int numChunks = (totalLength + CHUNK_SIZE - 1) / CHUNK_SIZE;
        List<CompletableFuture<Void>> subTasks = new ArrayList<>(numChunks);

        for (int cursor = 0; cursor < totalLength; cursor += CHUNK_SIZE) {
            int currentChunkSize = Math.min(CHUNK_SIZE, totalLength - cursor);
            long physicalOffset = offset + cursor;
            ByteBuffer chunkView = view.slice(cursor, currentChunkSize);

            subTasks.add(CompletableFuture.runAsync(() -> {
                cancelled.checkNotCancelled();
                try {
                    seekableReader.readFully(physicalOffset, chunkView);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            }, executor));
        }
        return CompletableFuture.allOf(subTasks.toArray(new CompletableFuture[0]));
    }

}
