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
 * Reads a file section in parallel by splitting it into chunks and reading each
 * chunk concurrently. The chunks are read from the underlying
 * {@link SeekableReader} and written into the provided {@link ByteBuffer}. The
 * reading is done asynchronously using a provided {@link Executor}.
 */
class ParallelReader {

    public static final int CHUNK_SIZE = 8 * 1024 * 1024; // 8 MB

    private final SeekableReader seekableReader;
    private final Executor executor;

    ParallelReader(SeekableReader seekableReader, Executor executor) {
        this.seekableReader = seekableReader;
        this.executor = executor;
    }

    /**
     * Fills {@code view} (from position 0 to its limit) with the file bytes
     * starting at {@code offset}, reading chunks of up to {@code CHUNK_SIZE} bytes
     * concurrently.
     */
    public CompletableFuture<Void> readChunked(long offset, ByteBuffer view, Cancellation cancelled) {
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
