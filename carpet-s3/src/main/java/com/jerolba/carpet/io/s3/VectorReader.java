package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VectorReader implements RangeReader {

    private static final Logger logger = LoggerFactory.getLogger(VectorReader.class);

    private static final int CHUNK_SIZE = 8 * 1024 * 1024; // 8 MB

    private final SeekableReader seekableReader;
    private final Executor executor;

    VectorReader(SeekableReader seekableReader, Executor executor) {
        this.seekableReader = seekableReader;
        this.executor = executor;
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
        for (ParquetFileRange range : ranges) {
            if (range.getLength() > CHUNK_SIZE) {
                readRangeInParallelChunks(allocator, range);
            } else {
                readRange(allocator, range);
            }
        }
    }

    private void readRangeInParallelChunks(ByteBufferAllocator allocator, ParquetFileRange range) {
        long startOffset = range.getOffset();
        int totalLength = range.getLength();
        int numChunks = (totalLength + CHUNK_SIZE - 1) / CHUNK_SIZE;

        ByteBuffer buffer = allocator.allocate(totalLength);
        List<CompletableFuture<Void>> subTasks = new ArrayList<>(numChunks);

        for (int cursor = 0; cursor < totalLength; cursor += CHUNK_SIZE) {
            int currentChunkSize = Math.min(CHUNK_SIZE, totalLength - cursor);
            long physicalOffset = startOffset + cursor;
            ByteBuffer chunkView = buffer.slice(cursor, currentChunkSize);

            subTasks.add(CompletableFuture.runAsync(() -> {
                long init = System.nanoTime();
                logger.debug("readRangeInParallelChunks() - Reading chunk: offset={}, length={}",
                        physicalOffset, currentChunkSize);
                try {
                    seekableReader.readFully(physicalOffset, chunkView);
                    logger.debug(
                            "readRangeInParallelChunks() - Finished chunk: offset={}, length={}, speed={} MB/s",
                            physicalOffset, currentChunkSize, (currentChunkSize / (1024.0 * 1024.0))
                                    / ((System.nanoTime() - init) / 1_000_000_000.0));
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            }, executor));
        }

        CompletableFuture<ByteBuffer> combined = CompletableFuture.allOf(subTasks.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    buffer.position(0);
                    buffer.limit(totalLength);
                    return buffer;
                })
                .exceptionally(t -> {
                    allocator.release(buffer);
                    throw new CompletionException(t);
                });
        range.setDataReadFuture(combined);
    }

    private void readRange(ByteBufferAllocator allocator, ParquetFileRange range) {
        CompletableFuture<ByteBuffer> future = CompletableFuture.supplyAsync(() -> {
            long init = System.nanoTime();
            logger.debug("readVectored() - Reading range asynchronously: offset={}, length={}",
                    range.getOffset(), range.getLength());
            try {
                ByteBuffer rangeToBuffer = readRangeToBuffer(range, allocator);
                logger.debug(
                        "readVectored() - Finished reading range asynchronously: offset={}, length={}, speed={} MB/s",
                        range.getOffset(), range.getLength(), (range.getLength() / (1024.0 * 1024.0))
                                / ((System.nanoTime() - init) / 1_000_000_000.0));
                return rangeToBuffer;
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        }, executor);
        range.setDataReadFuture(future);
    }

    @Override
    public void close() {
        if (executor instanceof OwnedExecutor executorService) {
            executorService.shutdown();
        }
    }

    private ByteBuffer readRangeToBuffer(ParquetFileRange range, ByteBufferAllocator allocator) throws IOException {
        long offset = range.getOffset();
        int length = range.getLength();
        ByteBuffer byteBuffer = allocator.allocate(length);
        try {
            seekableReader.readFully(offset, byteBuffer);
            byteBuffer.flip();
            return byteBuffer;
        } catch (IOException e) {
            allocator.release(byteBuffer);
            throw e;
        }
    }
}
