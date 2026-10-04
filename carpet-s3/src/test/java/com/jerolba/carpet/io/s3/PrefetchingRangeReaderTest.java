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

import static com.jerolba.carpet.io.s3.PrefetchTestSupport.allColumns;
import static com.jerolba.carpet.io.s3.PrefetchTestSupport.inputFileOf;
import static com.jerolba.carpet.io.s3.PrefetchTestSupport.multiRowGroupParquet;
import static com.jerolba.carpet.io.s3.PrefetchTestSupport.rangesFor;
import static com.jerolba.carpet.io.s3.PrefetchTestSupport.rowGroupsOf;
import static com.jerolba.carpet.io.s3.PrefetchTestSupport.rows;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.readerOf;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.sequentialBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.io.s3.PrefetchTestSupport.ChunkGeometry;
import com.jerolba.carpet.io.s3.PrefetchTestSupport.ManualExecutor;
import com.jerolba.carpet.io.s3.PrefetchTestSupport.NarrowRow;
import com.jerolba.carpet.io.s3.PrefetchTestSupport.RecordingReader;
import com.jerolba.carpet.io.s3.PrefetchTestSupport.TestRow;
import com.jerolba.carpet.io.s3.PrefetchTestSupport.TrackingAllocator;
import com.jerolba.carpet.io.s3.PrefetchingRangeReader.Stats;

class PrefetchingRangeReaderTest {

    private static final int ROW_COUNT = 5000;
    private static final int LARGE_ROW_COUNT = 1_200_000;
    private static final int UNLIMITED = Integer.MAX_VALUE;

    private static byte[] parquet;
    private static List<List<ChunkGeometry>> geometry;
    private static int rowGroups;

    @BeforeAll
    static void setUp() throws IOException {
        parquet = multiRowGroupParquet(ROW_COUNT, 16 * 1024);
        geometry = rowGroupsOf(parquet);
        rowGroups = geometry.size();
        assertTrue(rowGroups >= 4, "Test file needs at least 4 row groups, got " + rowGroups);
    }

    /**
     * A prefetching reader over the in-memory file, with a manual executor so the
     * test decides when each download completes.
     * <p>
     * The test file is smaller than the footer cache, so every read would be served
     * from the cache. The reads the range reader issues are therefore recorded
     * above the cache ({@code recording}); {@code underlying} records what reaches
     * the raw file, which only matters for the footer tests.
     * </p>
     */
    private static class Harness implements AutoCloseable {

        final RecordingReader underlying;
        final CachedFooterProxyReader footer;
        final RecordingReader recording;
        final ManualExecutor executor = new ManualExecutor();
        final TrackingAllocator allocator = new TrackingAllocator();
        final PrefetchingRangeReader reader;

        Harness(long maxBytes) throws IOException {
            this(parquet, maxBytes, 8 * 1024 * 1024);
        }

        Harness(byte[] data, long maxBytes, int footerCacheSize) throws IOException {
            this.underlying = new RecordingReader(readerOf(data));
            this.footer = new CachedFooterProxyReader(underlying, footerCacheSize);
            underlying.clearReads();
            this.recording = new RecordingReader(footer);
            this.reader = new PrefetchingRangeReader(executor, recording, maxBytes);
        }

        /** Requests the given columns of a row group as parquet-java would. */
        List<ParquetFileRange> request(int rowGroup, int... columns) throws IOException {
            return request(allocator, rowGroup, columns);
        }

        List<ParquetFileRange> request(ByteBufferAllocator alloc, int rowGroup, int... columns) throws IOException {
            List<ChunkGeometry> group = geometry.get(rowGroup);
            List<ParquetFileRange> ranges = rangesFor(group, columns.length == 0 ? allColumns(group) : columns);
            reader.readVectored(ranges, alloc);
            return ranges;
        }

        /** The single range covering a whole row group. */
        ParquetFileRange wholeRowGroup(int rowGroup) {
            List<ChunkGeometry> group = geometry.get(rowGroup);
            List<ParquetFileRange> ranges = rangesFor(group, allColumns(group));
            assertEquals(1, ranges.size(), "row group chunks are expected to be consecutive");
            return ranges.get(0);
        }

        Stats stats() {
            return reader.stats();
        }

        void assertData(List<ParquetFileRange> ranges) throws Exception {
            for (ParquetFileRange range : ranges) {
                ByteBuffer buffer = range.getDataReadFuture().get(5, TimeUnit.SECONDS);
                byte[] actual = new byte[buffer.remaining()];
                buffer.duplicate().get(actual);
                byte[] expected = Arrays.copyOfRange(parquet, (int) range.getOffset(),
                        (int) (range.getOffset() + range.getLength()));
                assertArrayEquals(expected, actual, "data of " + range);
            }
        }

        @Override
        public void close() {
            reader.close();
            assertEquals(List.of(), allocator.violations());
        }
    }

    private Harness harness;

    @BeforeEach
    void createHarness() throws IOException {
        harness = new Harness(UNLIMITED);
    }

    @AfterEach
    void closeHarness() {
        harness.close();
    }

    @Nested
    class Hits {

        @Test
        void nextRowGroupIsReadBeforeBeingRequested() throws Exception {
            harness.request(0);
            harness.executor.runPending();

            assertEquals(1, harness.stats().speculationsIssued());
            assertEquals(1, harness.executor.pending(), "one speculative download queued");
            harness.executor.runPending();

            ParquetFileRange rowGroup1 = harness.wholeRowGroup(1);
            assertEquals(1, harness.recording.countReadsAt(rowGroup1.getOffset()));
        }

        @Test
        void exactMatchIsServedWithoutNewReads() throws Exception {
            harness.request(0);
            harness.executor.runAll();

            List<ParquetFileRange> rowGroup1 = harness.request(1);

            Stats stats = harness.stats();
            assertEquals(1, stats.rangesServed());
            assertEquals(1, stats.rangesFetched());
            assertEquals(0, stats.bytesDiscarded());
            assertEquals(0, stats.inFlightWaits(), "data was ready, nothing to wait for");
            assertTrue(rowGroup1.get(0).getDataReadFuture().isDone());
            harness.assertData(rowGroup1);
            assertEquals(1, harness.recording.countReadsAt(rowGroup1.get(0).getOffset()));
        }

        @Test
        void inFlightSpeculationIsHandedOverWithoutBlocking() throws Exception {
            harness.request(0);
            harness.executor.runPending();
            assertEquals(1, harness.executor.pending(), "speculation queued but not started");

            List<ParquetFileRange> rowGroup1 = harness.request(1);

            assertEquals(1, harness.stats().rangesServed());
            assertFalse(rowGroup1.get(0).getDataReadFuture().isDone(), "readVectored must not block");
            harness.executor.runAll();
            harness.assertData(rowGroup1);

            Stats stats = harness.stats();
            assertEquals(1, stats.inFlightWaits(), "the reader waited once for an in-flight speculation");
            assertTrue(stats.inFlightWaitNanos() > 0);
        }

        @Test
        void rangeContainedInSpeculationIsSliced() throws Exception {
            harness.request(0);
            harness.executor.runAll();

            List<ParquetFileRange> column1 = harness.request(1, 1);

            assertEquals(1, harness.stats().rangesServed());
            assertEquals(0, harness.stats().bytesDiscarded());
            harness.assertData(column1);
            assertEquals(1, harness.recording.countReadsAt(harness.wholeRowGroup(1).getOffset()));
        }

        @Test
        void severalRequestedRangesCanShareOneSpeculatedBuffer() throws Exception {
            harness.request(0);
            harness.executor.runAll();

            List<ParquetFileRange> columns = harness.request(1, 0, 2);

            assertEquals(2, columns.size());
            assertEquals(2, harness.stats().rangesServed());
            assertEquals(0, harness.stats().bytesDiscarded());
            harness.assertData(columns);
        }

        @Test
        void speculationChainsAcrossAllRowGroups() throws Exception {
            for (int rowGroup = 0; rowGroup < rowGroups; rowGroup++) {
                List<ParquetFileRange> ranges = harness.request(rowGroup);
                harness.executor.runAll();
                harness.assertData(ranges);
            }

            Stats stats = harness.stats();
            assertEquals(rowGroups - 1, stats.speculationsIssued());
            assertEquals(rowGroups - 1, stats.rangesServed());
            assertEquals(1, stats.rangesFetched());
            assertEquals(0, stats.bytesDiscarded());
            for (int rowGroup = 0; rowGroup < rowGroups; rowGroup++) {
                assertEquals(1, harness.recording.countReadsAt(harness.wholeRowGroup(rowGroup).getOffset()),
                        "row group " + rowGroup + " read exactly once");
            }
        }

        @Test
        void projectionIsKeptAcrossRowGroups() throws Exception {
            harness.request(0, 1, 2);
            harness.executor.runAll();

            List<ParquetFileRange> ranges = harness.request(1, 1, 2);

            assertEquals(1, harness.stats().rangesServed());
            harness.assertData(ranges);
        }
    }

    @Nested
    class Misses {

        @Test
        void partialOverlapIsFetchedFreshAndSpeculationDiscarded() throws Exception {
            harness.request(0, 1, 2);
            harness.executor.runAll();
            long speculated = harness.wholeRowGroup(1).getLength() - geometry.get(1).get(0).length()
                    - geometry.get(1).get(3).length();

            List<ParquetFileRange> rowGroup1 = harness.request(1);
            harness.executor.runAll();

            Stats stats = harness.stats();
            assertEquals(0, stats.rangesServed());
            assertEquals(2, stats.rangesFetched());
            assertEquals(speculated, stats.bytesDiscarded());
            assertEquals(1, harness.allocator.releasedCount(), "discarded speculative buffer released");
            harness.assertData(rowGroup1);
        }

        @Test
        void nearbyProjectedColumnsAreSpeculatedAsOneSpanLikeTheFreshPath() throws Exception {
            harness.request(0, 0, 2);
            harness.executor.runAll();
            assertEquals(1, harness.stats().speculationsIssued());
            List<ChunkGeometry> rowGroup1 = geometry.get(1);
            int spanLength = (int) (rowGroup1.get(2).end() - rowGroup1.get(0).start());
            assertTrue(harness.recording.hasReadCovering(rowGroup1.get(0).start(), spanLength),
                    "columns 0 to 2 downloaded as a single span, gap included");
            assertEquals(2, harness.allocator.allocatedCount(), "one buffer per row group, as without prefetch");

            List<ParquetFileRange> columns = harness.request(1, 0, 2);

            assertEquals(2, columns.size());
            assertEquals(2, harness.stats().rangesServed());
            assertEquals(0, harness.stats().bytesDiscarded());
            harness.assertData(columns);
        }

        @Test
        void projectionGrowthStitchesTheSpeculatedPrefix() throws Exception {
            harness.request(0, 0, 2);
            harness.executor.runAll();

            List<ParquetFileRange> rowGroup1 = harness.request(1);
            harness.executor.runAll();

            Stats stats = harness.stats();
            assertEquals(1, stats.rangesServed(), "span of columns 0 to 2 is reused as prefix");
            assertEquals(0, stats.bytesDiscarded());
            harness.assertData(rowGroup1);
            assertEquals(1, harness.allocator.releasedCount(), "prefix buffer released after the copy");
            assertEquals(1, harness.recording.countReadsAt(geometry.get(1).get(3).start()), "only the tail fetched");
        }

        @Test
        void skippedRowGroupDiscardsSpeculationAndRestartsFromTheNewOne() throws Exception {
            harness.request(0);
            harness.executor.runAll();

            List<ParquetFileRange> rowGroup2 = harness.request(2);

            Stats afterSkip = harness.stats();
            assertEquals(0, afterSkip.rangesServed());
            assertEquals(2, afterSkip.rangesFetched());
            assertEquals(harness.wholeRowGroup(1).getLength(), afterSkip.bytesDiscarded());
            assertEquals(1, harness.allocator.releasedCount());

            harness.executor.runAll();
            harness.assertData(rowGroup2);
            assertEquals(2, harness.stats().speculationsIssued(), "row group 3 speculated after row group 2");
            assertEquals(1, harness.recording.countReadsAt(harness.wholeRowGroup(3).getOffset()));
        }

        @Test
        void allocatorChangeDiscardsSpeculation() throws Exception {
            harness.request(0);
            harness.executor.runAll();

            TrackingAllocator other = new TrackingAllocator();
            List<ParquetFileRange> rowGroup1 = harness.request(other, 1);
            harness.executor.runAll();

            assertEquals(0, harness.stats().rangesServed());
            assertEquals(harness.wholeRowGroup(1).getLength(), harness.stats().bytesDiscarded());
            assertEquals(1, harness.allocator.releasedCount(), "released through the allocator that allocated it");
            assertEquals(2, other.allocatedCount(), "fresh row group 1 plus speculative row group 2");
            harness.assertData(rowGroup1);
            assertEquals(List.of(), other.violations());
        }

        @Test
        void pageFilteredRequestDoesNotSpeculate() throws Exception {
            ParquetFileRange whole = harness.wholeRowGroup(0);
            List<ParquetFileRange> partial = List.of(
                    new ParquetFileRange(whole.getOffset() + 1, whole.getLength() - 1));

            harness.reader.readVectored(partial, harness.allocator);
            harness.executor.runAll();

            assertEquals(0, harness.stats().speculationsIssued());
            harness.assertData(partial);
        }

        @Test
        void lastRowGroupDoesNotSpeculate() throws Exception {
            List<ParquetFileRange> last = harness.request(rowGroups - 1);
            harness.executor.runAll();

            assertEquals(0, harness.stats().speculationsIssued());
            harness.assertData(last);
        }

        @Test
        void emptyRequestIsIgnored() throws Exception {
            harness.reader.readVectored(List.of(), harness.allocator);

            assertEquals(0, harness.executor.pending());
            assertEquals(0, harness.stats().speculationsIssued());
        }
    }

    @Nested
    class Cancellation {

        @Test
        void notStartedSpeculationIsCancelledOnMismatch() throws Exception {
            harness.request(0);
            harness.executor.runPending();
            assertEquals(1, harness.executor.pending(), "speculation queued");

            harness.request(2);
            harness.executor.runAll();

            assertEquals(0, harness.recording.countReadsAt(harness.wholeRowGroup(1).getOffset()),
                    "cancelled speculation never reaches the reader");
            assertEquals(3, harness.allocator.allocatedCount(), "row groups 0, 2 and speculated 3");
            assertEquals(harness.wholeRowGroup(1).getLength(), harness.stats().bytesDiscarded());
        }

        @Test
        void closeCancelsPendingSpeculation() throws Exception {
            harness.request(0);
            harness.executor.runPending();
            assertEquals(1, harness.executor.pending());

            harness.reader.close();
            harness.executor.runAll();

            assertEquals(0, harness.recording.countReadsAt(harness.wholeRowGroup(1).getOffset()));
            assertEquals(1, harness.allocator.allocatedCount());
        }

        @Test
        void closeReleasesCompletedSpeculation() throws Exception {
            harness.request(0);
            harness.executor.runAll();
            assertEquals(2, harness.allocator.allocatedCount());

            harness.reader.close();

            assertEquals(1, harness.allocator.releasedCount());
            assertEquals(harness.wholeRowGroup(1).getLength(), harness.allocator.releasedBytes());
        }

        @Test
        void completionAfterCloseDoesNotSpeculate() throws Exception {
            List<ParquetFileRange> rowGroup0 = harness.request(0);
            harness.reader.close();

            harness.executor.runAll();

            assertEquals(0, harness.stats().speculationsIssued());
            assertEquals(0, harness.executor.pending());
            harness.assertData(rowGroup0);
        }

        @Test
        void closeIsIdempotent() throws Exception {
            harness.request(0);
            harness.executor.runAll();

            harness.reader.close();
            assertDoesNotThrow(harness.reader::close);
            assertEquals(1, harness.allocator.releasedCount());
        }
    }

    @Nested
    class Failures {

        @Test
        void speculativeFailureIsRetriedTransparently() throws Exception {
            long rowGroup1Offset = harness.wholeRowGroup(1).getOffset();
            harness.recording.failOnceAt(rowGroup1Offset);
            harness.request(0);
            harness.executor.runAll();

            List<ParquetFileRange> rowGroup1 = harness.request(1);
            assertEquals(1, harness.stats().rangesServed());
            harness.executor.runAll();

            harness.assertData(rowGroup1);
            assertEquals(2, harness.recording.countReadsAt(rowGroup1Offset), "failed speculation plus fresh fetch");
        }

        @Test
        void failureOfTheCurrentRowGroupDoesNotSpeculate() throws Exception {
            ParquetFileRange rowGroup0 = harness.wholeRowGroup(0);
            harness.recording.failOnceAt(rowGroup0.getOffset());

            List<ParquetFileRange> ranges = harness.request(0);
            harness.executor.runAll();

            assertThrows(ExecutionException.class, () -> ranges.get(0).getDataReadFuture().get());
            assertEquals(0, harness.stats().speculationsIssued());
        }

        @Test
        void nonParquetFileNeverSpeculates() throws Exception {
            try (Harness plain = new Harness(sequentialBytes(4096), UNLIMITED, 1024)) {
                List<ParquetFileRange> ranges = List.of(new ParquetFileRange(100, 500));

                plain.reader.readVectored(ranges, plain.allocator);
                plain.executor.runAll();

                assertEquals(0, plain.stats().speculationsIssued());
                ByteBuffer buffer = ranges.get(0).getDataReadFuture().get(5, TimeUnit.SECONDS);
                assertEquals(500, buffer.remaining());
            }
        }

        @Test
        void footerLargerThanCacheIsFetchedInBackground() throws Exception {
            try (Harness tinyCache = new Harness(parquet, UNLIMITED, 16)) {
                tinyCache.request(0);
                tinyCache.executor.runAll();

                assertEquals(1, tinyCache.stats().speculationsIssued());
                long footerReads = tinyCache.underlying.reads().stream()
                        .filter(r -> r.pos() + r.len() == parquet.length - 8)
                        .count();
                assertEquals(1, footerReads, "footer body fetched once from the underlying reader");
            }
        }

        @Test
        void encryptedFooterDisablesPrefetchQuietly() throws Exception {
            byte[] encrypted = parquet.clone();
            System.arraycopy("PARE".getBytes(), 0, encrypted, encrypted.length - 4, 4);
            try (Harness harness = new Harness(encrypted, UNLIMITED, 8 * 1024 * 1024)) {
                harness.request(0);
                harness.executor.runAll();

                assertEquals(0, harness.stats().speculationsIssued());
            }
        }
    }

    @Nested
    class MemoryCap {

        @Test
        void capCutsTheSpeculationAndTheTailIsStitched() throws Exception {
            List<ChunkGeometry> rowGroup1 = geometry.get(1);
            long cap = rowGroup1.get(0).length() + 1;
            try (Harness capped = new Harness(cap)) {
                capped.request(0);
                capped.executor.runAll();
                assertEquals(1, capped.stats().speculationsIssued());
                assertEquals(1, capped.recording.countReadsAt(rowGroup1.get(0).start()));

                List<ParquetFileRange> ranges = capped.request(1);
                capped.executor.runAll();

                assertEquals(1, capped.stats().rangesServed());
                capped.assertData(ranges);
                assertEquals(1, capped.allocator.releasedCount(), "prefix buffer released after the copy");
                assertEquals(1, capped.recording.countReadsAt(rowGroup1.get(1).start()), "tail fetched once");
            }
        }

        @Test
        void capSmallerThanFirstChunkSkipsSpeculation() throws Exception {
            try (Harness capped = new Harness(geometry.get(1).get(0).length() - 1)) {
                capped.request(0);
                capped.executor.runAll();

                assertEquals(0, capped.stats().speculationsIssued());
            }
        }

        @Test
        void stitchedPrefixFailureIsRetriedTransparently() throws Exception {
            List<ChunkGeometry> rowGroup1 = geometry.get(1);
            try (Harness capped = new Harness(rowGroup1.get(0).length() + 1)) {
                capped.recording.failOnceAt(rowGroup1.get(0).start());
                capped.request(0);
                capped.executor.runAll();

                List<ParquetFileRange> ranges = capped.request(1);
                capped.executor.runAll();

                capped.assertData(ranges);
                assertEquals(2, capped.recording.countReadsAt(rowGroup1.get(0).start()));
            }
        }
    }

    @Nested
    class EndToEnd {

        private ExecutorService pool;
        private List<PrefetchingRangeReader> prefetchers;
        private RecordingReader recording;
        private InputFile file;

        @BeforeEach
        void setUp() {
            pool = Executors.newFixedThreadPool(4);
            prefetchers = new ArrayList<>();
            recording = new RecordingReader(readerOf(parquet));
            file = inputFileOf(recording, parquet.length, pool, new PrefetchConfig(true, UNLIMITED), prefetchers);
        }

        @AfterEach
        void tearDown() {
            pool.shutdownNow();
        }

        private Stats aggregatedStats() {
            return aggregate(prefetchers);
        }

        /**
         * CarpetReader may open more than one stream; the counters are summed over all
         * of them.
         */
        private static Stats aggregate(List<PrefetchingRangeReader> readers) {
            long issued = 0, served = 0, fetched = 0, servedBytes = 0, discarded = 0, waits = 0, waitNanos = 0;
            for (PrefetchingRangeReader reader : readers) {
                Stats stats = reader.stats();
                issued += stats.speculationsIssued();
                served += stats.rangesServed();
                fetched += stats.rangesFetched();
                servedBytes += stats.bytesServed();
                discarded += stats.bytesDiscarded();
                waits += stats.inFlightWaits();
                waitNanos += stats.inFlightWaitNanos();
            }
            return new Stats(issued, served, fetched, servedBytes, discarded, waits, waitNanos);
        }

        @Test
        void carpetReaderReadsIdenticalRecordsWithPrefetch() throws IOException {
            List<TestRow> read = new CarpetReader<>(file, TestRow.class).toList();

            assertEquals(rows(ROW_COUNT), read);
            Stats stats = aggregatedStats();
            assertEquals(rowGroups - 1, stats.speculationsIssued());
            assertEquals(rowGroups - 1, stats.rangesServed());
            assertEquals(1, stats.rangesFetched());
            assertEquals(0, stats.bytesDiscarded());
        }

        @Test
        void fileLargerThanFooterCacheDownloadsEachRowGroupOnce() throws IOException {
            byte[] large = multiRowGroupParquet(LARGE_ROW_COUNT, 1024 * 1024);
            assertTrue(large.length > 8 * 1024 * 1024 + 1024 * 1024,
                    "file must exceed the footer cache to observe real reads, got " + large.length);
            List<List<ChunkGeometry>> largeGeometry = rowGroupsOf(large);
            assertTrue(largeGeometry.size() >= 3);
            RecordingReader rawReads = new RecordingReader(readerOf(large));
            List<PrefetchingRangeReader> largePrefetchers = new ArrayList<>();
            InputFile largeFile = inputFileOf(rawReads, large.length, pool, new PrefetchConfig(true, UNLIMITED),
                    largePrefetchers);

            List<TestRow> read = new CarpetReader<>(largeFile, TestRow.class).toList();

            assertEquals(rows(LARGE_ROW_COUNT), read);
            Stats stats = aggregate(largePrefetchers);
            assertEquals(largeGeometry.size() - 1, stats.rangesServed());
            assertEquals(1, stats.rangesFetched());
            for (int rowGroup = 0; rowGroup < largeGeometry.size(); rowGroup++) {
                List<ChunkGeometry> group = largeGeometry.get(rowGroup);
                ParquetFileRange whole = rangesFor(group, allColumns(group)).get(0);
                if (whole.getOffset() + whole.getLength() <= large.length - 8 * 1024 * 1024) {
                    assertTrue(rawReads.hasReadCovering(whole.getOffset(), whole.getLength()),
                            "row group " + rowGroup + " must be downloaded");
                    assertEquals(1, rawReads.countReadsAt(whole.getOffset()),
                            "row group " + rowGroup + " downloaded once");
                }
            }
        }

        @Test
        void projectedReadIsServedFromSpeculation() throws IOException {
            List<NarrowRow> read = new CarpetReader<>(file, NarrowRow.class).toList();

            assertEquals(ROW_COUNT, read.size());
            assertEquals(rows(ROW_COUNT).stream().map(r -> new NarrowRow(r.id(), r.name())).toList(), read);
            Stats stats = aggregatedStats();
            assertEquals(rowGroups - 1, stats.speculationsIssued());
            assertEquals(rowGroups - 1, stats.rangesServed());
            assertEquals(1, stats.rangesFetched());
        }

        @Test
        void earlyCloseWithSpeculationInFlightDoesNotFail() {
            assertDoesNotThrow(() -> {
                try (Stream<TestRow> stream = new CarpetReader<>(file, TestRow.class).stream()) {
                    assertEquals(10, stream.limit(10).count());
                }
            });
        }
    }

}
