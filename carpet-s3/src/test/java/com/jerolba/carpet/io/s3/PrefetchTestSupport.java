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

import static com.jerolba.carpet.io.s3.PrefetchConfig.DEFAULT_MAX_BYTES;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.readerOf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.io.SeekableInputStream;

import com.jerolba.carpet.CarpetWriter;

/**
 * Test doubles and fixtures for the speculative prefetch tests.
 */
class PrefetchTestSupport {

    record TestRow(long id, String name, double value, int count) {
    }

    record NarrowRow(long id, String name) {
    }

    static List<TestRow> rows(int count) {
        List<TestRow> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(new TestRow(i, "name-" + (i % 97), i * 1.5, i % 13));
        }
        return rows;
    }

    /**
     * Writes a Parquet file with several row groups.
     */
    static byte[] multiRowGroupParquet(int rowCount, long rowGroupSize) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var writer = new CarpetWriter.Builder<>(baos, TestRow.class).withRowGroupSize(rowGroupSize).build()) {
            writer.write(rows(rowCount));
        }
        return baos.toByteArray();
    }

    /** Position and size of a column chunk, as parquet-java sees it. */
    record ChunkGeometry(long start, long length) {

        long end() {
            return start + length;
        }
    }

    /**
     * Column chunk geometry of every row group, obtained through parquet-java
     * itself so that the tests do not depend on {@link ParquetFileLayout}.
     */
    static List<List<ChunkGeometry>> rowGroupsOf(byte[] parquet) throws IOException {
        PrefetchConfig config = new PrefetchConfig(false, DEFAULT_MAX_BYTES);
        try (ParquetFileReader reader = ParquetFileReader.open(inputFileOf(parquet, null, config))) {
            List<List<ChunkGeometry>> result = new ArrayList<>();
            for (BlockMetaData block : reader.getRowGroups()) {
                List<ChunkGeometry> chunks = new ArrayList<>();
                for (ColumnChunkMetaData column : block.getColumns()) {
                    chunks.add(new ChunkGeometry(column.getStartingPos(), column.getTotalSize()));
                }
                result.add(chunks);
            }
            return result;
        }
    }

    static int[] allColumns(List<ChunkGeometry> rowGroup) {
        int[] columns = new int[rowGroup.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = i;
        }
        return columns;
    }

    /**
     * The ranges parquet-java requests for the given columns of a row group:
     * consecutive chunks are merged into one range.
     */
    static List<ParquetFileRange> rangesFor(List<ChunkGeometry> rowGroup, int... columns) {
        List<ParquetFileRange> ranges = new ArrayList<>();
        long start = -1;
        long end = -1;
        for (int column : columns) {
            ChunkGeometry chunk = rowGroup.get(column);
            if (start >= 0 && chunk.start() == end) {
                end = chunk.end();
            } else {
                if (start >= 0) {
                    ranges.add(new ParquetFileRange(start, (int) (end - start)));
                }
                start = chunk.start();
                end = chunk.end();
            }
        }
        if (start >= 0) {
            ranges.add(new ParquetFileRange(start, (int) (end - start)));
        }
        return ranges;
    }

    static long totalLength(List<ParquetFileRange> ranges) {
        return ranges.stream().mapToLong(ParquetFileRange::getLength).sum();
    }

    static InputFile inputFileOf(byte[] data, Executor executor, PrefetchConfig config) {
        return inputFileOf(readerOf(data), data.length, executor, config, new ArrayList<>());
    }

    /**
     * An {@link InputFile} whose streams are assembled exactly as
     * {@code S3InputFileImpl} assembles them. Every {@link PrefetchingRangeReader}
     * created for a stream is added to {@code prefetchers}, so a test can read its
     * statistics without the stream exposing it.
     */
    static InputFile inputFileOf(SeekableReader reader, long length, Executor executor, PrefetchConfig config,
            List<PrefetchingRangeReader> prefetchers) {
        return new InputFile() {
            @Override
            public long getLength() {
                return length;
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                CachedFooterProxyReader cached = new CachedFooterProxyReader(reader);
                RangeReader rangeReader = config.create(cached, executor);
                if (rangeReader instanceof PrefetchingRangeReader prefetching) {
                    prefetchers.add(prefetching);
                }
                return new SeekableCachedFooterReader(cached, rangeReader);
            }
        };
    }

    /**
     * Executor that only runs tasks when told to, so tests decide exactly when a
     * download completes.
     */
    static final class ManualExecutor implements Executor {

        private final Deque<Runnable> queue = new ConcurrentLinkedDeque<>();

        @Override
        public void execute(Runnable command) {
            queue.add(command);
        }

        int pending() {
            return queue.size();
        }

        boolean runNext() {
            Runnable task = queue.poll();
            if (task == null) {
                return false;
            }
            task.run();
            return true;
        }

        /** Runs the tasks queued at this moment, not the ones they enqueue. */
        int runPending() {
            int count = queue.size();
            for (int i = 0; i < count; i++) {
                runNext();
            }
            return count;
        }

        /** Runs tasks until the queue is empty, including newly enqueued ones. */
        int runAll() {
            int count = 0;
            while (runNext()) {
                count++;
            }
            return count;
        }
    }

    /**
     * Heap allocator that records allocations and releases and detects releases of
     * foreign buffers or double releases. Violations are recorded instead of thrown
     * because releases happen inside future callbacks, where an exception would be
     * swallowed.
     */
    static final class TrackingAllocator implements ByteBufferAllocator {

        private final List<ByteBuffer> allocated = Collections.synchronizedList(new ArrayList<>());
        private final List<ByteBuffer> released = Collections.synchronizedList(new ArrayList<>());
        private final List<String> violations = Collections.synchronizedList(new ArrayList<>());

        @Override
        public ByteBuffer allocate(int size) {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            allocated.add(buffer);
            return buffer;
        }

        @Override
        public void release(ByteBuffer buffer) {
            if (!containsIdentity(allocated, buffer)) {
                violations.add("Released a buffer not allocated here (slice or foreign): " + buffer);
                return;
            }
            if (containsIdentity(released, buffer)) {
                violations.add("Double release of " + buffer);
                return;
            }
            released.add(buffer);
        }

        @Override
        public boolean isDirect() {
            return false;
        }

        int allocatedCount() {
            return allocated.size();
        }

        int releasedCount() {
            return released.size();
        }

        long releasedBytes() {
            synchronized (released) {
                return released.stream().mapToLong(ByteBuffer::capacity).sum();
            }
        }

        List<String> violations() {
            return List.copyOf(violations);
        }

        private static boolean containsIdentity(List<ByteBuffer> buffers, ByteBuffer buffer) {
            synchronized (buffers) {
                for (ByteBuffer candidate : buffers) {
                    if (candidate == buffer) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * Reader that records every read and can fail the first read at given offsets.
     */
    static final class RecordingReader implements SeekableReader {

        record Read(long pos, int len) {
        }

        private final SeekableReader delegate;
        private final List<Read> reads = Collections.synchronizedList(new ArrayList<>());
        private final Set<Long> failOnceAt = ConcurrentHashMap.newKeySet();

        RecordingReader(SeekableReader delegate) {
            this.delegate = delegate;
        }

        void failOnceAt(long offset) {
            failOnceAt.add(offset);
        }

        void clearReads() {
            reads.clear();
        }

        List<Read> reads() {
            return List.copyOf(reads);
        }

        long countReadsAt(long pos) {
            return reads().stream().filter(r -> r.pos() == pos).count();
        }

        boolean hasReadCovering(long pos, int len) {
            return reads().stream().anyMatch(r -> r.pos() <= pos && r.pos() + r.len() >= pos + len);
        }

        @Override
        public long getLength() throws IOException {
            return delegate.getLength();
        }

        @Override
        public int readFully(long pos, byte[] b, int off, int len) throws IOException {
            record(pos, len);
            return delegate.readFully(pos, b, off, len);
        }

        @Override
        public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
            record(pos, byteBuffer.remaining());
            return delegate.readFully(pos, byteBuffer);
        }

        private void record(long pos, int len) throws IOException {
            reads.add(new Read(pos, len));
            if (failOnceAt.remove(pos)) {
                throw new IOException("Injected failure at " + pos);
            }
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

}
