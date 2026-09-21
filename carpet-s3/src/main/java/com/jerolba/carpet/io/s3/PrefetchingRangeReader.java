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

import static com.jerolba.carpet.io.s3.Cancellation.NEVER_CANCELLED;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jerolba.carpet.io.s3.ParquetFileLayout.ByteRange;
import com.jerolba.carpet.io.s3.ParquetFileLayout.Projection;
import com.jerolba.carpet.io.s3.PrefetchCounters.Stats;
import com.jerolba.carpet.io.s3.Speculation.PrefetchedRange;

/**
 * A {@link RangeReader} decorator that speculatively reads the next row group
 * while the reader decodes the current one.
 * <p>
 * parquet-java issues one {@code readVectored} call per row group, with one
 * range per run of consecutive projected column chunks, and only comes back
 * for the next row group after every record of the current one has been
 * consumed. This reader uses that window: once all ranges of a call have been
 * downloaded, it maps them to the row group and columns they cover using the
 * file footer, predicts the ranges of the following row group and issues them
 * through the delegate. When the next call arrives, ranges already prefetched
 * are served from the speculation and only the rest goes to the network.
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
 * callback that issues the next speculation. The one outstanding speculation
 * and the closed state live in a single atomic reference, so they always change
 * together; the footer layout is decoded once under a lock. Nothing else is
 * shared mutably.
 * </p>
 */
class PrefetchingRangeReader extends RangeReader {

    private static final Logger logger = LoggerFactory.getLogger(PrefetchingRangeReader.class);

    private final VectorReader delegate;
    private final ParallelReader parallelReader;
    private final SeekableReader footerSource;
    private final long maxBytes;

    /** Marks {@link #current} once the reader is closed; it is never replaced afterwards. */
    private static final Speculation CLOSED = new Speculation(-1, null, List.of());

    /** The single outstanding speculation, null if none, or {@link #CLOSED}. */
    private final AtomicReference<Speculation> current = new AtomicReference<>();

    private final ReentrantLock layoutLock = new ReentrantLock();
    /** Null until the first attempt; empty when the layout is unavailable. */
    private volatile Optional<ParquetFileLayout> layout = null;

    private final PrefetchCounters counters = new PrefetchCounters();

    /**
     * @param delegate       reader that performs the physical reads on its
     *                       executor; speculation needs background downloads, so
     *                       only the vectored reader qualifies
     * @param parallelReader reader that fills a caller-provided buffer, used to
     *                       complete a range of which only a prefix was
     *                       prefetched
     * @param footerSource   reader used to obtain the footer bytes, typically one
     *                       with the footer already cached
     * @param maxBytes       maximum number of bytes a speculation may hold
     */
    PrefetchingRangeReader(VectorReader delegate, ParallelReader parallelReader, SeekableReader footerSource,
            long maxBytes) {
        this.delegate = delegate;
        this.parallelReader = parallelReader;
        this.footerSource = footerSource;
        this.maxBytes = maxBytes;
    }

    @Override
    void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
        if (ranges.isEmpty()) {
            return;
        }
        Speculation speculation = takeCurrent();
        List<ParquetFileRange> misses = new ArrayList<>();
        if (speculation == null) {
            misses.addAll(ranges);
        } else if (speculation.allocator() != allocator) {
            logger.debug("Allocator changed, discarding speculation of row group {}", speculation.rowGroup());
            discard(speculation);
            misses.addAll(ranges);
        } else {
            for (ParquetFileRange range : ranges) {
                CompletableFuture<ByteBuffer> served = new RangeRequest(range, allocator).serve(speculation);
                if (served != null) {
                    range.setDataReadFuture(served);
                    counters.rangeServed(range.getLength());
                } else {
                    misses.add(range);
                }
            }
            // Releases only what the reader did not claim
            discard(speculation);
            logger.debug("Row group {} speculation: {} ranges served, {} fetched", speculation.rowGroup(),
                    ranges.size() - misses.size(), misses.size());
        }
        if (!misses.isEmpty()) {
            counters.rangesFetched(misses.size());
            delegate.readVectored(misses, allocator);
        }
        gateAndScheduleNextSpeculation(ranges, allocator);
    }

    @Override
    CompletableFuture<ByteBuffer> readSpan(ByteBufferAllocator allocator, long offset, int length,
            Cancellation cancelled) {
        return delegate.readSpan(allocator, offset, length, cancelled);
    }

    @Override
    public void close() {
        Speculation speculation = current.getAndSet(CLOSED);
        if (speculation != null && speculation != CLOSED) {
            logger.debug("Closing with an unconsumed speculation of row group {}", speculation.rowGroup());
            discard(speculation);
        }
        logger.debug("Prefetch stats: {}", stats());
        delegate.close();
    }

    /**
     * Snapshot of the counters, for diagnostics and tests.
     */
    Stats stats() {
        return counters.snapshot();
    }

    // -------------------------------------------------------------------------
    // Serving a request from the speculation
    // -------------------------------------------------------------------------

    /**
     * One range requested by the reader, resolved against the speculation.
     */
    private final class RangeRequest {

        private final long offset;
        private final int length;
        private final ByteBufferAllocator allocator;

        RangeRequest(ParquetFileRange range, ByteBufferAllocator allocator) {
            this.offset = range.getOffset();
            this.length = range.getLength();
            this.allocator = allocator;
        }

        /**
         * @return a future with the range content, or null if the speculation does
         *         not hold it
         */
        CompletableFuture<ByteBuffer> serve(Speculation speculation) {
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
                logger.debug("Speculative read of [{}, {}) failed, fetching again", offset, offset + length,
                        failure);
                // An exception thrown here fails the returned future, like a failed read
                return delegate.readSpan(allocator, offset, length, NEVER_CANCELLED);
            }).thenCompose(Function.identity());
        }

        /**
         * Builds the requested range from a prefetched prefix plus a fresh read of
         * the tail, both landing in a single buffer. Happens only when the
         * speculation was cut by the memory cap.
         */
        private CompletableFuture<ByteBuffer> stitch(PrefetchedRange prefix) {
            int prefixLength = prefix.length();
            ByteBuffer buffer = allocator.allocate(length);
            CompletableFuture<Void> tail = readIntoQuietly(offset + prefixLength,
                    buffer.slice(prefixLength, length - prefixLength));
            CompletableFuture<Void> head = prefix.future().handle((prefetched, failure) -> {
                if (failure == null) {
                    buffer.put(0, prefetched, prefetched.position(), prefixLength);
                    allocator.release(prefetched);
                    return CompletableFuture.<Void>completedFuture(null);
                }
                logger.debug("Speculative read of [{}, {}) failed, fetching again", offset, offset + prefixLength,
                        failure);
                return readIntoQuietly(offset, buffer.slice(0, prefixLength));
            }).thenCompose(Function.identity());
            return CompletableFuture.allOf(head, tail)
                    .thenApply(v -> {
                        buffer.position(0);
                        buffer.limit(length);
                        return buffer;
                    })
                    .exceptionally(failure -> {
                        allocator.release(buffer);
                        throw failure instanceof CompletionException ce ? ce : new CompletionException(failure);
                    });
        }

    }

    private CompletableFuture<Void> readIntoQuietly(long offset, ByteBuffer target) {
        try {
            return parallelReader.readChunked(offset, target, NEVER_CANCELLED);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // -------------------------------------------------------------------------
    // Issuing the next speculation
    // -------------------------------------------------------------------------

    /**
     * Issues the next speculation when every range of this call has been read,
     * and gates the futures handed to the reader on that moment. parquet-java
     * needs all ranges of a row group before it can proceed, so the gate adds no
     * latency, and it guarantees the speculation is published before the reader
     * can ask for the next row group.
     * <p>
     * The gate always completes normally, whatever happens while speculating: a
     * failure of the speculation, even an {@link Error} such as an
     * {@link OutOfMemoryError} while allocating its buffers, must never fail the
     * ranges the reader asked for. Each range still fails with its own read
     * failure, if any.
     * </p>
     */
    private void gateAndScheduleNextSpeculation(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) {
        List<ByteRange> requested = new ArrayList<>(ranges.size());
        List<CompletableFuture<ByteBuffer>> originals = new ArrayList<>(ranges.size());
        for (ParquetFileRange range : ranges) {
            CompletableFuture<ByteBuffer> future = range.getDataReadFuture();
            if (future == null) {
                return;
            }
            requested.add(new ByteRange(range.getOffset(), range.getLength()));
            originals.add(future);
        }
        CompletableFuture<Void> gate = CompletableFuture.allOf(originals.toArray(new CompletableFuture[0]))
                .handle((v, failure) -> {
                    if (failure == null) {
                        onRowGroupComplete(requested, allocator);
                    } else {
                        logger.debug("Row group read failed, no speculation issued", failure);
                    }
                    return null;
                });
        for (int i = 0; i < ranges.size(); i++) {
            CompletableFuture<ByteBuffer> original = originals.get(i);
            ranges.get(i).setDataReadFuture(gate.thenCompose(v -> original));
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
            int next = projection.rowGroup() + 1;
            if (next >= fileLayout.rowGroupCount()) {
                return;
            }
            List<ByteRange> predicted = fileLayout.predict(next, projection.columns(), maxBytes, RANGE_GAP);
            if (predicted.isEmpty() || isClosed()) {
                return;
            }
            Speculation speculation = issue(next, predicted, allocator);
            counters.speculationIssued();
            logger.debug("Speculatively reading row group {}: {} ranges, {} bytes", next, predicted.size(),
                    speculation.totalBytes());
            publish(speculation);
        } catch (RejectedExecutionException e) {
            logger.debug("Executor rejected the speculative read, the stream is probably closed");
        } catch (Exception e) {
            logger.warn("Speculative read failed, continuing without prefetch", e);
        } catch (Error e) {
            // Speculation is optional work: an Error raised by it, typically an
            // OutOfMemoryError allocating the speculative buffers, is not allowed to fail
            // the row group the reader requested
            logger.warn("Speculative read failed with an error, continuing without prefetch", e);
        }
    }

    /**
     * Issues one delegate span read per predicted span, each with its own cancellation
     * flag so that discarding one span never stops another one that was handed
     * over. Spans are already merged with the delegate's gap rule, so the
     * speculation performs the same physical reads the reader would.
     */
    private Speculation issue(int rowGroup, List<ByteRange> predicted, ByteBufferAllocator allocator) {
        List<PrefetchedRange> prefetched = new ArrayList<>(predicted.size());
        try {
            for (ByteRange range : predicted) {
                AtomicBoolean cancelled = new AtomicBoolean(false);
                CompletableFuture<ByteBuffer> future = delegate.readSpan(allocator, range.offset(), range.length(),
                        cancelled::get);
                prefetched.add(new PrefetchedRange(range.offset(), range.length(), future, cancelled));
            }
        } catch (RuntimeException | Error e) {
            // Release the spans already issued before giving up
            discard(new Speculation(rowGroup, allocator, prefetched));
            throw e;
        }
        return new Speculation(rowGroup, allocator, prefetched);
    }

    private void publish(Speculation speculation) {
        Speculation stale = current.getAndUpdate(c -> c == CLOSED ? CLOSED : speculation);
        if (stale == CLOSED) {
            logger.debug("Stream closed while speculating, discarding row group {}", speculation.rowGroup());
            discard(speculation);
        } else if (stale != null) {
            logger.warn("Replacing an unconsumed speculation of row group {}", stale.rowGroup());
            discard(stale);
        }
    }

    // -------------------------------------------------------------------------
    // State helpers
    // -------------------------------------------------------------------------

    private Speculation takeCurrent() {
        Speculation speculation = current.getAndUpdate(c -> c == CLOSED ? CLOSED : null);
        return speculation == CLOSED ? null : speculation;
    }

    private boolean isClosed() {
        return current.get() == CLOSED;
    }

    private void discard(Speculation speculation) {
        long discarded = speculation.discard();
        counters.bytesDiscarded(discarded);
    }

    private ParquetFileLayout layout() {
        Optional<ParquetFileLayout> resolved = layout;
        if (resolved == null) {
            // Not synchronized: the footer may be read from S3, which would pin a virtual
            // thread to its carrier on JDK 21 to 23
            layoutLock.lock();
            try {
                resolved = layout;
                if (resolved == null) {
                    resolved = loadLayout();
                    layout = resolved;
                }
            } finally {
                layoutLock.unlock();
            }
        }
        return resolved.orElse(null);
    }

    private Optional<ParquetFileLayout> loadLayout() {
        try {
            ParquetFileLayout loaded = ParquetFileLayout.read(footerSource);
            logger.debug("Decoded footer for prefetch: {} row groups", loaded.rowGroupCount());
            return Optional.of(loaded);
        } catch (ParquetFileLayout.UnsupportedLayoutException e) {
            logger.info("Prefetch disabled for this file: {}", e.getMessage());
            return Optional.empty();
        } catch (IOException | RuntimeException e) {
            logger.debug("Prefetch disabled for this file: footer could not be decoded", e);
            return Optional.empty();
        }
    }

}
