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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jerolba.carpet.io.s3.ParquetFileLayout.ByteRange;
import com.jerolba.carpet.io.s3.ParquetFileLayout.Projection;
import com.jerolba.carpet.io.s3.ParquetFileLayout.UnsupportedLayoutException;
import com.jerolba.carpet.io.s3.Speculation.PrefetchedRange;

/**
 * A {@link RangeReader} decorator that speculatively reads the next row group
 * while the reader decodes the current one.
 * <p>
 * parquet-java issues one {@code readVectored} call per row group, with one
 * range per run of consecutive projected column chunks, and only comes back for
 * the next row group after every record of the current one has been consumed.
 * This reader uses that window: once all ranges of a call have been downloaded,
 * it maps them to the row group and columns they cover using the file footer,
 * predicts the ranges of the following row group and issues them through the
 * delegate. When the next call arrives, ranges already prefetched are served
 * from the speculation and only the rest goes to the network.
 * </p>
 * <p>
 * The reader never blocks in {@code readVectored}: an in-flight speculative
 * future is handed over as is, and parquet-java waits on it exactly as it waits
 * for a fresh read. A speculative read that fails is retried once with a fresh
 * read before the failure reaches the reader. Whatever is prefetched and not
 * requested is discarded and its memory released.
 * </p>
 * <p>
 * Thread safety: the reader thread calls {@code readVectored} and
 * {@code close}; executor threads complete downloads and run the completion
 * callback that issues the next speculation. A single lock guards the closed
 * flag and the one outstanding speculation; nothing else is shared mutably and
 * no I/O happens under the lock.
 * </p>
 */
class PrefetchingRangeReader extends RangeReader {

    private static final Logger logger = LoggerFactory.getLogger(PrefetchingRangeReader.class);

    private final VectorReader delegate;
    private final ParallelReader parallelReader;
    private final SeekableReader footerReader;
    private final long maxBytes;

    private final Object lock = new Object();
    /** Guarded by {@link #lock}. */
    private boolean closed = false;
    /** Guarded by {@link #lock}. The single outstanding speculation, if any. */
    private Speculation current = null;

    private final Object layoutLock = new Object();
    /** Null until the first attempt; empty when the layout is unavailable. */
    private volatile Optional<ParquetFileLayout> layout = null;

    private final Counters counters = new Counters();

    /**
     * @param delegate       reader that performs the physical reads on its
     *                       executor; speculation needs background downloads, so
     *                       only the vectored reader qualifies
     * @param seekableReader reader used to obtain the footer bytes, typically one
     *                       with the footer already cached
     * @param maxBytes       maximum number of bytes a speculation may hold
     */
    PrefetchingRangeReader(Executor executor, SeekableReader seekableReader, long maxBytes) {
        this.delegate = new VectorReader(seekableReader, executor);
        this.parallelReader = new ParallelReader(seekableReader, executor);
        this.footerReader = seekableReader;
        this.maxBytes = maxBytes;
    }

    @Override
    void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator, Cancellation cancellation)
            throws IOException {
        if (ranges.isEmpty()) {
            return;
        }
        Speculation speculation = takeCurrent();
        List<ParquetFileRange> misses = new ArrayList<>();
        int servedInFlight = 0;
        if (speculation == null) {
            misses.addAll(ranges);
        } else {
            for (ParquetFileRange range : ranges) {
                CompletableFuture<ByteBuffer> served = new SpeculationServe(range, allocator).serve(speculation);
                if (served != null) {
                    range.setDataReadFuture(served);
                    counters.addBytesServed(range.getLength());
                    if (!served.isDone()) {
                        servedInFlight++;
                    }
                } else {
                    misses.add(range);
                }
            }
            // Releases only what the reader did not claim
            discard(speculation);
        }
        if (!misses.isEmpty()) {
            counters.addRangesFetched(misses.size());
            delegate.readVectored(misses, allocator, cancellation);
        }
        CallSummary summary = new CallSummary(describe(speculation, ranges), ranges.size() - misses.size(),
                servedInFlight, misses.size());
        gateAndScheduleNextSpeculation(ranges, allocator, summary);
    }

    /** What a readVectored call was served with, for the wait trace. */
    private record CallSummary(String subject, int served, int servedInFlight, int fetched) {

        /**
         * Traces how long the reader was blocked on this call: from the moment the
         * futures were handed over until the last range landed, which is when
         * parquet-java can proceed. A call served from a speculation still in flight is
         * the case the prefetch could not fully hide, so it is also counted.
         */
        void traceWait(Counters counters, long waitedNanos) {
            if (fetched == 0 && servedInFlight == 0) {
                logger.debug("{}: {} ranges ready from speculation, no wait", subject, served);
            } else if (fetched == 0) {
                counters.incrementInFlightWaits();
                counters.addInFlightWaitNanos(waitedNanos);
                logger.debug("{}: {} ranges from speculation, {} still downloading, reader waited {} ms",
                        subject, served, servedInFlight, millis(waitedNanos));
            } else {
                logger.debug("{}: {} ranges from speculation, {} fetched fresh, reader waited {} ms",
                        subject, served, fetched, millis(waitedNanos));
            }
        }

        private static String millis(long nanos) {
            return String.format("%.1f", nanos / 1_000_000.0);
        }

    }

    private static String describe(Speculation speculation, List<ParquetFileRange> ranges) {
        return speculation != null
                ? "Row group " + speculation.rowGroup()
                : "Ranges at offset " + ranges.get(0).getOffset();
    }

    @Override
    protected CompletableFuture<ByteBuffer> readSpan(ByteBufferAllocator allocator, long offset, int length,
            Cancellation cancellation) {
        return delegate.readSpan(allocator, offset, length, cancellation);
    }

    @Override
    public void close() {
        Speculation speculation;
        synchronized (lock) {
            closed = true;
            speculation = current;
            current = null;
        }
        if (speculation != null) {
            logger.debug("Closing with an unconsumed speculation of row group {}", speculation.rowGroup());
            discard(speculation);
        }
        logger.debug("Prefetch stats: {}", counters.stats());
        delegate.close();
    }

    /**
     * @param inFlightWaits     calls served entirely from a speculation that had
     *                          not finished downloading when the reader asked
     * @param inFlightWaitNanos total time the reader waited for those downloads
     */
    record Stats(long speculationsIssued, long rangesServed, long rangesFetched, long bytesServed,
            long bytesDiscarded, long inFlightWaits, long inFlightWaitNanos) {
    }

    // -------------------------------------------------------------------------
    // Serving a request from the speculation
    // -------------------------------------------------------------------------

    private class SpeculationServe {

        private final long offset;
        private final int length;
        private final ByteBufferAllocator allocator;

        public SpeculationServe(ParquetFileRange range, ByteBufferAllocator allocator) {
            this.offset = range.getOffset();
            this.length = range.getLength();
            this.allocator = allocator;
        }

        public CompletableFuture<ByteBuffer> serve(Speculation speculation) {
            PrefetchedRange covering = speculation.find(p -> p.covers(offset, length));
            if (covering != null) {
                // Several requested ranges may share one prefetched buffer; the first claim
                // hands the buffer over, later ones just slice it.
                covering.claim();
                int sliceOffset = (int) (offset - covering.offset());
                CompletableFuture<ByteBuffer> future = covering.future();
                if (sliceOffset != 0 || length != covering.length()) {
                    future = future.thenApply(buffer -> buffer.slice(sliceOffset, length));
                }
                return withFallback(future);
            }
            PrefetchedRange prefix = speculation.find(p -> p.offset() == offset && p.length() < length);
            if (prefix != null && prefix.claim()) {
                return stitch(prefix);
            }
            return null;
        }

        /**
         * A speculative failure must not fail a read the reader never asked for: on
         * exceptional completion the range is fetched again, once.
         */
        private CompletableFuture<ByteBuffer> withFallback(CompletableFuture<ByteBuffer> speculative) {
            return speculative.handle((buffer, failure) -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(buffer);
                }
                logger.debug("Speculative read of [{}, {}) failed, fetching again", offset, offset + length, failure);
                return fetchCompleteParquetFileRange();
            }).thenCompose(Function.identity()); // Flatmap futures
        }

        private CompletableFuture<ByteBuffer> fetchCompleteParquetFileRange() {
            ParquetFileRange fresh = new ParquetFileRange(offset, length);
            try {
                delegate.readVectored(List.of(fresh), allocator);
            } catch (IOException | RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
            return fresh.getDataReadFuture();
        }

        /**
         * Builds the requested range from a prefetched prefix plus a fresh read of the
         * tail, both landing in a single buffer. Happens only when the speculation was
         * cut by the memory cap.
         */
        private CompletableFuture<ByteBuffer> stitch(PrefetchedRange alreadyRead) {
            int alreadyReadLength = alreadyRead.length();
            // Allocate a buffer for all content
            ByteBuffer buffer = allocator.allocate(length);
            // Read tail of the buffer
            CompletableFuture<Void> tail = readIntoQuietly(offset + alreadyReadLength,
                    buffer.slice(alreadyReadLength, length - alreadyReadLength));

            CompletableFuture<Void> head = alreadyRead.future().handle((prefetched, failure) -> {
                if (failure != null) {
                    logger.debug("Speculative read of [{}, {}) failed, fetching again", offset,
                            offset + alreadyReadLength, failure);
                    // if failed read again head content into final buffer
                    return readIntoQuietly(offset, buffer.slice(0, alreadyReadLength));
                }
                // Copy head content into final buffer
                buffer.put(0, prefetched, prefetched.position(), alreadyReadLength);
                // Release buffer previously allocated with the head
                allocator.release(prefetched);
                // Return future with null because data is already copied into final buffer
                return CompletableFuture.<Void>completedFuture(null);
            }).thenCompose(Function.identity()); // Flatmap futures

            return CompletableFuture.allOf(head, tail)
                    .thenApply(v -> {
                        // with two segments loaded we can configure the buffer to be complete
                        buffer.position(0);
                        buffer.limit(length);
                        return buffer;
                    })
                    .exceptionally(failure -> {
                        allocator.release(buffer);
                        throw failure instanceof CompletionException ce ? ce : new CompletionException(failure);
                    });
        }

        /**
         * Reads {@code target.remaining()} bytes starting at {@code offset} into the
         * region of {@code target} between its position and its limit.
         *
         * The position and limit of {@code target} are not modified: the reads go
         * through slices.
         *
         * @param offset file position of the first byte
         * @param target destination buffer
         * @return a future completed when all bytes have been written
         */
        private CompletableFuture<Void> readIntoQuietly(long offset, ByteBuffer target) {
            try {
                return parallelReader.readChunked(offset, target.slice(), NEVER_CANCELLED);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Issuing the next speculation
    // -------------------------------------------------------------------------

    /**
     * Issues the next speculation when every range of this call has been read, and
     * gates the futures handed to the reader on that moment. parquet-java needs all
     * ranges of a row group before it can proceed, so the gate adds no latency, and
     * it guarantees the speculation is published before the reader can ask for the
     * next row group.
     */
    private void gateAndScheduleNextSpeculation(List<ParquetFileRange> requestedRanges, ByteBufferAllocator allocator,
            CallSummary summary) {
        var resolvedFutures = new ArrayList<CompletableFuture<ByteBuffer>>(requestedRanges.size());
        for (ParquetFileRange range : requestedRanges) {
            CompletableFuture<ByteBuffer> future = range.getDataReadFuture();
            if (future == null) {
                return;
            }
            resolvedFutures.add(future);
        }
        // We project next requested ranges based on current request ranges
        var pastRanges = requestedRanges.stream()
                .map(range -> new ByteRange(range.getOffset(), range.getLength()))
                .toList();
        long handedOverAt = System.nanoTime();
        CompletableFuture<Void> gate = CompletableFuture
                .allOf(resolvedFutures.toArray(new CompletableFuture[0]))
                .whenComplete((v, failure) -> {
                    if (failure == null) {
                        summary.traceWait(counters, System.nanoTime() - handedOverAt);
                        // When all ranges from the row group are completed, we can launch the next
                        // batch
                        onRowGroupComplete(pastRanges, allocator);
                    } else {
                        logger.debug("{}: read failed, no speculation issued", summary.subject(), failure);
                    }
                });

        // Wait for the completion of all request to make them available
        for (int i = 0; i < requestedRanges.size(); i++) {
            CompletableFuture<ByteBuffer> original = resolvedFutures.get(i);
            requestedRanges.get(i).setDataReadFuture(gate.thenCompose(v -> original));
        }
    }

    private void onRowGroupComplete(List<ByteRange> requested, ByteBufferAllocator allocator) {
        try {
            if (isClosed()) {
                return;
            }
            ParquetFileLayout fileLayout = layout();
            if (fileLayout == null) {
                return;
            }
            Projection projection = fileLayout.projectionOf(requested);
            if (projection == null) {
                logger.debug("Requested ranges do not cover whole column chunks, no speculation issued");
                return;
            }
            int nextRowGroup = projection.rowGroup() + 1;
            if (nextRowGroup >= fileLayout.rowGroupCount()) {
                return;
            }
            List<ByteRange> predicted = fileLayout.predict(nextRowGroup, projection.columns(), maxBytes, RANGE_GAP);
            if (predicted.isEmpty() || isClosed()) {
                return;
            }
            Speculation speculation = issue(nextRowGroup, predicted, allocator);
            counters.incrementSpeculationsIssued();
            logger.debug("Speculatively reading row group {}: {} ranges, {} bytes", nextRowGroup, predicted.size(),
                    speculation.totalBytes());
            publish(speculation);
        } catch (RejectedExecutionException e) {
            logger.debug("Executor rejected the speculative read, the stream is probably closed");
        } catch (Exception e) {
            logger.warn("Speculative read failed, continuing without prefetch", e);
        }
    }

    /**
     * Issues one delegate read per predicted span, each with its own cancellation
     * flag so that discarding one span never stops another one that was handed
     * over. Spans are already merged with the delegate's gap rule, so the
     * speculation performs the same physical reads the reader would.
     */
    private Speculation issue(int rowGroup, List<ByteRange> predicted, ByteBufferAllocator allocator)
            throws IOException {
        var prefetched = new ArrayList<PrefetchedRange>(predicted.size());
        var speculation = new Speculation(rowGroup, allocator, prefetched);
        try {
            for (ByteRange range : predicted) {
                AtomicBoolean cancelled = new AtomicBoolean(false);
                ParquetFileRange fileRange = new ParquetFileRange(range.offset(), range.length());

                // TODO: can we call to delegate.readSpan?
                delegate.readVectored(List.of(fileRange), allocator, cancelled::get);
                // TODO: is prefetched speculation modified?
                prefetched.add(new PrefetchedRange(range, fileRange.getDataReadFuture(), cancelled));
            }
            return speculation;
        } catch (IOException | RuntimeException e) {
            discard(speculation);
            throw e;
        }
    }

    private void publish(Speculation speculation) {
        Speculation stale;
        synchronized (lock) {
            if (closed) {
                stale = speculation;
            } else {
                stale = current;
                current = speculation;
            }
        }
        if (stale == speculation) {
            logger.debug("Stream closed while speculating, discarding row group {}", speculation.rowGroup());
            discard(stale);
        } else if (stale != null) {
            logger.warn("Replacing an unconsumed speculation of row group {}", stale.rowGroup());
            discard(stale);
        }
    }

    // -------------------------------------------------------------------------
    // State helpers
    // -------------------------------------------------------------------------

    private Speculation takeCurrent() {
        synchronized (lock) {
            Speculation speculation = current;
            current = null;
            return speculation;
        }
    }

    private boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    private void discard(Speculation speculation) {
        long discarded = speculation.discard();
        counters.addBytesDiscarded(discarded);
    }

    private ParquetFileLayout layout() {
        Optional<ParquetFileLayout> resolved = layout;
        if (resolved == null) {
            synchronized (layoutLock) {
                resolved = layout;
                if (resolved == null) {
                    resolved = loadLayout();
                    layout = resolved;
                }
            }
        }
        return resolved.orElse(null);
    }

    private Optional<ParquetFileLayout> loadLayout() {
        try {
            ParquetFileLayout loaded = ParquetFileLayout.read(footerReader);
            logger.debug("Decoded footer for prefetch: {} row groups", loaded.rowGroupCount());
            return Optional.of(loaded);
        } catch (UnsupportedLayoutException e) {
            logger.info("Prefetch disabled for this file: {}", e.getMessage());
            return Optional.empty();
        } catch (IOException | RuntimeException e) {
            logger.debug("Prefetch disabled for this file: footer could not be decoded", e);
            return Optional.empty();
        }
    }

    Stats stats() {
        return counters.stats();
    }

    private static class Counters {

        private final AtomicLong speculationsIssued = new AtomicLong();
        private final AtomicLong rangesServed = new AtomicLong();
        private final AtomicLong rangesFetched = new AtomicLong();
        private final AtomicLong bytesServed = new AtomicLong();
        private final AtomicLong bytesDiscarded = new AtomicLong();
        /** Calls served entirely from a speculation that was still downloading. */
        private final AtomicLong inFlightWaits = new AtomicLong();
        /** Time the reader spent waiting for those in-flight speculations to land. */
        private final AtomicLong inFlightWaitNanos = new AtomicLong();

        public void addBytesDiscarded(long discard) {
            bytesDiscarded.addAndGet(discard);
        }

        public void incrementSpeculationsIssued() {
            speculationsIssued.incrementAndGet();
        }

        public void addInFlightWaitNanos(long waitedNanos) {
            inFlightWaitNanos.addAndGet(waitedNanos);
        }

        public void incrementInFlightWaits() {
            inFlightWaits.incrementAndGet();
        }

        void addRangesFetched(int bytes) {
            rangesFetched.addAndGet(bytes);
        }

        void addBytesServed(int bytes) {
            rangesServed.incrementAndGet();
            bytesServed.addAndGet(bytes);

        }

        /**
         * Snapshot of the counters, for diagnostics and tests.
         */
        Stats stats() {
            return new Stats(speculationsIssued.get(), rangesServed.get(), rangesFetched.get(), bytesServed.get(),
                    bytesDiscarded.get(), inFlightWaits.get(), inFlightWaitNanos.get());
        }
    }
}
