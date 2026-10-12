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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.apache.parquet.bytes.ByteBufferAllocator;

/**
 * The speculative read of one row group: a set of ranges issued ahead of the
 * reader's request. Immutable once built; the only mutable state is the claim
 * and cancellation flag of each range.
 * <p>
 * A range's buffer has exactly one owner, decided by an atomic claim: either
 * the reader, when the range is handed over on a hit, or the discard path,
 * which cancels the read if it did not start yet and releases the buffer when
 * the read completes. A buffer can therefore never be released twice or handed
 * over after being released.
 * </p>
 */
final class Speculation {

    private final int rowGroup;
    private final ByteBufferAllocator allocator;
    private final List<PrefetchedRange> ranges;

    Speculation(int rowGroup, ByteBufferAllocator allocator, List<PrefetchedRange> ranges) {
        this.rowGroup = rowGroup;
        this.allocator = allocator;
        this.ranges = List.copyOf(ranges);
    }

    int rowGroup() {
        return rowGroup;
    }

    ByteBufferAllocator allocator() {
        return allocator;
    }

    long totalBytes() {
        long total = 0;
        for (PrefetchedRange range : ranges) {
            total += range.length();
        }
        return total;
    }

    PrefetchedRange find(Predicate<PrefetchedRange> predicate) {
        for (PrefetchedRange range : ranges) {
            if (predicate.test(range)) {
                return range;
            }
        }
        return null;
    }

    /**
     * Discards every range not yet claimed by the reader: pending reads are
     * cancelled before they start and completed buffers are released.
     *
     * @return the number of bytes discarded
     */
    long discard() {
        long discarded = 0;
        for (PrefetchedRange range : ranges) {
            if (range.discard(allocator)) {
                discarded += range.length();
            }
        }
        return discarded;
    }

    /**
     * One speculatively read range and its future buffer.
     */
    static final class PrefetchedRange {

        private final long offset;
        private final int length;
        private final CompletableFuture<ByteBuffer> future;
        private final AtomicBoolean cancelled;
        private final AtomicBoolean claimed = new AtomicBoolean(false);

        PrefetchedRange(long offset, int length, CompletableFuture<ByteBuffer> future, AtomicBoolean cancelled) {
            this.offset = offset;
            this.length = length;
            this.future = future;
            this.cancelled = cancelled;
        }

        long offset() {
            return offset;
        }

        int length() {
            return length;
        }

        long end() {
            return offset + length;
        }

        CompletableFuture<ByteBuffer> future() {
            return future;
        }

        boolean covers(long rangeOffset, int rangeLength) {
            return rangeOffset >= offset && rangeOffset + rangeLength <= end();
        }

        /**
         * Takes ownership of the buffer. Only the first caller succeeds.
         */
        boolean claim() {
            return claimed.compareAndSet(false, true);
        }

        /**
         * Claims the range for disposal: the read is cancelled if it has not started
         * and the buffer is released as soon as it exists.
         *
         * @return true if this call took ownership, false if the range was already
         *         claimed
         */
        boolean discard(ByteBufferAllocator allocator) {
            if (!claim()) {
                return false;
            }
            cancelled.set(true);
            future.whenComplete((buffer, failure) -> {
                if (buffer != null) {
                    allocator.release(buffer);
                }
            });
            return true;
        }

        @Override
        public String toString() {
            return String.format("prefetched[%,d - %,d)", offset, end());
        }
    }

}
