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

import static java.util.Comparator.comparingLong;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for reading Parquet file ranges from S3.
 * <p>
 * Implements vectored I/O by grouping ranges that are close together (within
 * {@code RANGE_GAP} bytes) into a single contiguous read span, reducing the
 * number of S3 requests. Each subclass defines how a span is physically read.
 * </p>
 */
abstract class RangeReader implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RangeReader.class);

    private static final int RANGE_GAP = 1024 * 1024; // 1 MB

    /**
     * Read the specified ranges from the Parquet file, using vectored I/O to
     * minimize the number of S3 requests. Ranges that are close together (within
     * {@code RANGE_GAP} bytes) will be merged into a single read.
     *
     * The resulting ByteBuffers for each range will be provided via the range's
     * data read future, which will complete when the data is available. The caller
     * is responsible for managing the lifecycle of the returned ByteBuffers,
     * including releasing them when no longer needed.
     *
     * @param ranges    the list of file ranges to read
     * @param allocator the ByteBuffer allocator to use for reading data
     * @throws IOException if an I/O error occurs during reading
     */
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
        if (ranges.isEmpty()) {
            return;
        }
        List<GroupRanges> rangesGroups = mergeCloseRanges(ranges);
        for (GroupRanges group : rangesGroups) {
            if (group.size() > 1) {
                logger.debug("readVectored() - Merging {} ranges into single read: offset={}, length={}",
                        group.size(), group.startOffset(), group.totalLength());
            }

            CompletableFuture<ByteBuffer> spanFuture = readSpan(allocator, group.startOffset(), group.totalLength());

            for (ParquetFileRange range : group) {
                int sliceOffset = group.sliceOffset(range);
                if (sliceOffset == 0 && range.getLength() == group.totalLength()) {
                    range.setDataReadFuture(spanFuture);
                } else {
                    range.setDataReadFuture(spanFuture.thenApply(buf -> buf.slice(sliceOffset, range.getLength())));
                }
            }
        }
    }

    private List<GroupRanges> mergeCloseRanges(List<ParquetFileRange> ranges) {
        List<ParquetFileRange> sorted = new ArrayList<>(ranges);
        sorted.sort(comparingLong(ParquetFileRange::getOffset));

        List<GroupRanges> groups = new ArrayList<>();
        List<ParquetFileRange> currentGroup = new ArrayList<>();
        currentGroup.add(sorted.get(0));
        long currentGroupEnd = sorted.get(0).getOffset() + sorted.get(0).getLength();

        for (int i = 1; i < sorted.size(); i++) {
            ParquetFileRange curr = sorted.get(i);
            long gap = curr.getOffset() - currentGroupEnd;
            if (gap <= RANGE_GAP) {
                currentGroup.add(curr);
                currentGroupEnd = Math.max(currentGroupEnd, curr.getOffset() + curr.getLength());
            } else {
                groups.add(new GroupRanges(currentGroup));
                currentGroup = new ArrayList<>();
                currentGroup.add(curr);
                currentGroupEnd = curr.getOffset() + curr.getLength();
            }
        }
        groups.add(new GroupRanges(currentGroup));
        return groups;
    }

    protected abstract CompletableFuture<ByteBuffer> readSpan(ByteBufferAllocator allocator, long offset, int length)
            throws IOException;

    @Override
    public abstract void close();

    /**
     * A group of {@link ParquetFileRange} objects that can be downloaded together.
     * Provides utility methods to calculate total length and individual slice
     * offsets relative to the start of the group.
     */
    private static class GroupRanges implements Iterable<ParquetFileRange> {

        private final List<ParquetFileRange> ranges;

        GroupRanges(List<ParquetFileRange> ranges) {
            this.ranges = ranges;
        }

        int size() {
            return ranges.size();
        }

        long startOffset() {
            return ranges.get(0).getOffset();
        }

        int totalLength() {
            ParquetFileRange last = ranges.get(ranges.size() - 1);
            return (int) (last.getOffset() + last.getLength() - startOffset());
        }

        int sliceOffset(ParquetFileRange range) {
            return (int) (range.getOffset() - startOffset());
        }

        @Override
        public Iterator<ParquetFileRange> iterator() {
            return ranges.iterator();
        }
    }
}
