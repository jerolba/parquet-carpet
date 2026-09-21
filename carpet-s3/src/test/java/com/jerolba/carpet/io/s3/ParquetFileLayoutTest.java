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
import static com.jerolba.carpet.io.s3.PrefetchTestSupport.multiRowGroupParquet;
import static com.jerolba.carpet.io.s3.PrefetchTestSupport.rangesFor;
import static com.jerolba.carpet.io.s3.PrefetchTestSupport.rowGroupsOf;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.readerOf;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.sequentialBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.io.s3.ParquetFileLayout.ByteRange;
import com.jerolba.carpet.io.s3.ParquetFileLayout.Chunk;
import com.jerolba.carpet.io.s3.ParquetFileLayout.Projection;
import com.jerolba.carpet.io.s3.ParquetFileLayout.UnsupportedLayoutException;
import com.jerolba.carpet.io.s3.PrefetchTestSupport.ChunkGeometry;
import com.jerolba.carpet.io.s3.PrefetchTestSupport.RecordingReader;

class ParquetFileLayoutTest {

    private static byte[] parquet;
    private static List<List<ChunkGeometry>> geometry;
    private static ParquetFileLayout layout;

    @BeforeAll
    static void setUp() throws IOException {
        parquet = multiRowGroupParquet(5000, 16 * 1024);
        geometry = rowGroupsOf(parquet);
        assertTrue(geometry.size() >= 4, "Test file needs at least 4 row groups, got " + geometry.size());
        layout = ParquetFileLayout.read(readerOf(parquet));
    }

    private static List<ByteRange> toByteRanges(List<ParquetFileRange> ranges) {
        return ranges.stream().map(r -> new ByteRange(r.getOffset(), r.getLength())).toList();
    }

    private static BitSet columns(int... columns) {
        BitSet set = new BitSet();
        for (int column : columns) {
            set.set(column);
        }
        return set;
    }

    @Nested
    class Decoding {

        @Test
        void rowGroupCountMatchesParquetJava() {
            assertEquals(geometry.size(), layout.rowGroupCount());
        }

        @Test
        void chunkBoundariesMatchParquetJava() {
            for (int rowGroup = 0; rowGroup < geometry.size(); rowGroup++) {
                List<ChunkGeometry> expected = geometry.get(rowGroup);
                List<Chunk> actual = layout.chunks(rowGroup);
                assertEquals(expected.size(), actual.size());
                for (int column = 0; column < expected.size(); column++) {
                    assertEquals(expected.get(column).start(), actual.get(column).offset(),
                            "offset of row group " + rowGroup + " column " + column);
                    assertEquals(expected.get(column).length(), actual.get(column).length(),
                            "length of row group " + rowGroup + " column " + column);
                    assertEquals(rowGroup, actual.get(column).rowGroup());
                    assertEquals(column, actual.get(column).column());
                }
            }
        }

        @Test
        void footerIsServedFromTheFooterCacheWithoutNewReads() throws IOException {
            RecordingReader recording = new RecordingReader(readerOf(parquet));
            CachedFooterProxyReader cached = new CachedFooterProxyReader(recording);
            recording.clearReads();

            ParquetFileLayout fromCache = ParquetFileLayout.read(cached);

            assertEquals(layout.rowGroupCount(), fromCache.rowGroupCount());
            assertTrue(recording.reads().isEmpty(), "footer decode must not hit the underlying reader");
        }

        @Test
        void footerLargerThanCacheIsReadFromTheUnderlyingReader() throws IOException {
            RecordingReader recording = new RecordingReader(readerOf(parquet));
            CachedFooterProxyReader tinyCache = new CachedFooterProxyReader(recording, 16);
            recording.clearReads();

            ParquetFileLayout fromReader = ParquetFileLayout.read(tinyCache);

            assertEquals(layout.rowGroupCount(), fromReader.rowGroupCount());
            assertEquals(1, recording.reads().size(), "one read for the footer body (tail is cached)");
        }

        @Test
        void encryptedFooterIsUnsupported() {
            byte[] encrypted = parquet.clone();
            byte[] magic = "PARE".getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(magic, 0, encrypted, encrypted.length - 4, 4);

            assertThrows(UnsupportedLayoutException.class, () -> ParquetFileLayout.read(readerOf(encrypted)));
        }

        @Test
        void nonParquetBytesAreRejected() {
            IOException ex = assertThrows(IOException.class,
                    () -> ParquetFileLayout.read(readerOf(sequentialBytes(1000))));
            assertFalse(ex instanceof UnsupportedLayoutException);
        }

        @Test
        void tooShortFileIsRejected() {
            assertThrows(IOException.class, () -> ParquetFileLayout.read(readerOf(new byte[5])));
        }

        @Test
        void corruptedFooterLengthIsRejected() {
            byte[] corrupted = parquet.clone();
            int lengthIndex = corrupted.length - 8;
            corrupted[lengthIndex] = (byte) 0xFF;
            corrupted[lengthIndex + 1] = (byte) 0xFF;
            corrupted[lengthIndex + 2] = (byte) 0xFF;
            corrupted[lengthIndex + 3] = (byte) 0x7F;

            assertThrows(IOException.class, () -> ParquetFileLayout.read(readerOf(corrupted)));
        }
    }

    @Nested
    class ProjectionOf {

        @Test
        void wholeRowGroupWithAllColumns() {
            List<ChunkGeometry> rowGroup = geometry.get(1);
            List<ParquetFileRange> ranges = rangesFor(rowGroup, allColumns(rowGroup));

            Projection projection = layout.projectionOf(toByteRanges(ranges));

            assertNotNull(projection);
            assertEquals(1, projection.rowGroup());
            assertEquals(columns(allColumns(rowGroup)), projection.columns());
        }

        @Test
        void subsetOfColumns() {
            List<ParquetFileRange> ranges = rangesFor(geometry.get(2), 0, 2);

            Projection projection = layout.projectionOf(toByteRanges(ranges));

            assertNotNull(projection);
            assertEquals(2, projection.rowGroup());
            assertEquals(columns(0, 2), projection.columns());
        }

        @Test
        void rangeNotStartingAtChunkBoundaryIsRejected() {
            ChunkGeometry chunk = geometry.get(0).get(0);
            List<ByteRange> ranges = List.of(new ByteRange(chunk.start() + 1, (int) chunk.length() - 1));

            assertNull(layout.projectionOf(ranges));
        }

        @Test
        void rangeShorterThanChunkIsRejected() {
            ChunkGeometry chunk = geometry.get(0).get(0);
            List<ByteRange> ranges = List.of(new ByteRange(chunk.start(), (int) chunk.length() - 1));

            assertNull(layout.projectionOf(ranges));
        }

        @Test
        void rangesFromDifferentRowGroupsAreRejected() {
            List<ByteRange> ranges = List.of(
                    toByteRanges(rangesFor(geometry.get(0), 0)).get(0),
                    toByteRanges(rangesFor(geometry.get(1), 0)).get(0));

            assertNull(layout.projectionOf(ranges));
        }

        @Test
        void emptyRangesAreRejected() {
            assertNull(layout.projectionOf(List.of()));
        }
    }

    @Nested
    class Predict {

        private static final long NO_GAP = 0;
        private static final long GAP = RangeReader.RANGE_GAP;

        @Test
        void consecutiveColumnsMergeIntoOneRange() {
            List<ChunkGeometry> rowGroup = geometry.get(1);

            List<ByteRange> predicted = layout.predict(1, columns(allColumns(rowGroup)), Long.MAX_VALUE, NO_GAP);

            assertEquals(toByteRanges(rangesFor(rowGroup, allColumns(rowGroup))), predicted);
            assertEquals(1, predicted.size());
        }

        @Test
        void nonConsecutiveColumnsProduceSeparateRangesWithoutGapMerging() {
            List<ChunkGeometry> rowGroup = geometry.get(1);

            List<ByteRange> predicted = layout.predict(1, columns(0, 2), Long.MAX_VALUE, NO_GAP);

            assertEquals(toByteRanges(rangesFor(rowGroup, 0, 2)), predicted);
            assertEquals(2, predicted.size());
        }

        @Test
        void nearbyColumnsMergeAcrossTheGapLikeTheRangeReader() {
            List<ChunkGeometry> rowGroup = geometry.get(1);
            ChunkGeometry first = rowGroup.get(0);
            ChunkGeometry third = rowGroup.get(2);

            List<ByteRange> predicted = layout.predict(1, columns(0, 2), Long.MAX_VALUE, GAP);

            assertEquals(List.of(new ByteRange(first.start(), (int) (third.end() - first.start()))), predicted);
        }

        @Test
        void gapBytesCountAgainstTheCap() {
            List<ChunkGeometry> rowGroup = geometry.get(1);
            long spanned = rowGroup.get(0).length() + rowGroup.get(1).length() + rowGroup.get(2).length();

            List<ByteRange> underCap = layout.predict(1, columns(0, 2), spanned - 1, GAP);
            List<ByteRange> atCap = layout.predict(1, columns(0, 2), spanned, GAP);

            assertEquals(List.of(new ByteRange(rowGroup.get(0).start(), (int) rowGroup.get(0).length())), underCap);
            assertEquals(1, atCap.size());
            assertEquals(spanned, atCap.get(0).length());
        }

        @Test
        void capCutsAtChunkGranularityKeepingAPrefix() {
            List<ChunkGeometry> rowGroup = geometry.get(1);
            long cap = rowGroup.get(0).length() + rowGroup.get(1).length() / 2;

            List<ByteRange> predicted = layout.predict(1, columns(allColumns(rowGroup)), cap, GAP);

            assertEquals(List.of(new ByteRange(rowGroup.get(0).start(), (int) rowGroup.get(0).length())), predicted);
        }

        @Test
        void capSmallerThanFirstChunkPredictsNothing() {
            List<ChunkGeometry> rowGroup = geometry.get(1);

            List<ByteRange> predicted = layout.predict(1, columns(allColumns(rowGroup)),
                    rowGroup.get(0).length() - 1, GAP);

            assertTrue(predicted.isEmpty());
        }

        @Test
        void columnBeyondRowGroupPredictsNothing() {
            assertTrue(layout.predict(1, columns(0, 99), Long.MAX_VALUE, GAP).isEmpty());
        }
    }

    @Nested
    class FromMetadata {

        private ColumnChunk chunk(long dataPageOffset, long dictionaryPageOffset, long size) {
            ColumnMetaData meta = new ColumnMetaData();
            meta.setData_page_offset(dataPageOffset);
            if (dictionaryPageOffset > 0) {
                meta.setDictionary_page_offset(dictionaryPageOffset);
            }
            meta.setTotal_compressed_size(size);
            ColumnChunk chunk = new ColumnChunk();
            chunk.setFile_offset(dataPageOffset);
            chunk.setMeta_data(meta);
            return chunk;
        }

        private FileMetaData metadata(RowGroup... rowGroups) {
            FileMetaData metadata = new FileMetaData();
            metadata.setRow_groups(Arrays.asList(rowGroups));
            return metadata;
        }

        private RowGroup rowGroup(ColumnChunk... chunks) {
            RowGroup rowGroup = new RowGroup();
            rowGroup.setColumns(Arrays.asList(chunks));
            return rowGroup;
        }

        @Test
        void dictionaryPageBeforeDataPageStartsTheChunk() throws IOException {
            ParquetFileLayout fromMeta = ParquetFileLayout.of(metadata(rowGroup(chunk(150, 100, 200))));

            assertEquals(100, fromMeta.chunks(0).get(0).offset());
            assertEquals(200, fromMeta.chunks(0).get(0).length());
        }

        @Test
        void dictionaryPageAfterDataPageIsIgnored() throws IOException {
            ParquetFileLayout fromMeta = ParquetFileLayout.of(metadata(rowGroup(chunk(100, 150, 200))));

            assertEquals(100, fromMeta.chunks(0).get(0).offset());
        }

        @Test
        void missingDictionaryOffsetUsesDataPageOffset() throws IOException {
            ParquetFileLayout fromMeta = ParquetFileLayout.of(metadata(rowGroup(chunk(4, 0, 50))));

            assertEquals(4, fromMeta.chunks(0).get(0).offset());
        }

        @Test
        void rowGroupWithAChunkTooLargeForVectoredIoIsNotPredicted() throws IOException {
            long huge = 3L * 1024 * 1024 * 1024;
            ParquetFileLayout fromMeta = ParquetFileLayout.of(metadata(
                    rowGroup(chunk(4, 0, 100)),
                    rowGroup(chunk(104, 0, 100), chunk(204, 0, huge), chunk(204 + huge, 0, 100))));

            assertTrue(fromMeta.predict(1, columns(0, 1, 2), Long.MAX_VALUE, RangeReader.RANGE_GAP).isEmpty());
        }

        @Test
        void overlappingChunksAreUnsupported() {
            FileMetaData metadata = metadata(rowGroup(chunk(100, 0, 50), chunk(100, 0, 60)));

            assertThrows(UnsupportedLayoutException.class, () -> ParquetFileLayout.of(metadata));
        }

        @Test
        void externalFilePathIsUnsupported() {
            ColumnChunk external = chunk(100, 0, 50);
            external.setFile_path("other.parquet");

            assertThrows(UnsupportedLayoutException.class, () -> ParquetFileLayout.of(metadata(rowGroup(external))));
        }

        @Test
        void missingColumnMetadataIsUnsupported() {
            ColumnChunk encrypted = new ColumnChunk();
            encrypted.setFile_offset(100);

            assertThrows(UnsupportedLayoutException.class,
                    () -> ParquetFileLayout.of(metadata(rowGroup(encrypted))));
        }

        @Test
        void noRowGroupsIsAnEmptyLayout() throws IOException {
            ParquetFileLayout fromMeta = ParquetFileLayout.of(new FileMetaData());

            assertEquals(0, fromMeta.rowGroupCount());
        }

        @Test
        void zeroLengthChunksAreSkippedInPrediction() throws IOException {
            ParquetFileLayout fromMeta = ParquetFileLayout.of(metadata(
                    rowGroup(chunk(4, 0, 10), chunk(14, 0, 0), chunk(14, 0, 20))));

            List<ByteRange> predicted = fromMeta.predict(0, columns(0, 1, 2), Long.MAX_VALUE, 0);

            assertEquals(List.of(new ByteRange(4, 30)), predicted);
        }
    }

}
