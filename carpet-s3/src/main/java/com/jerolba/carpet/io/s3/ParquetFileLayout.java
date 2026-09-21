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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;

/**
 * Physical layout of a Parquet file: where each column chunk of each row group
 * lives. Decoded directly from the thrift footer, without going through
 * parquet-hadoop, so that a plain byte range can be mapped back to the row
 * group and columns it belongs to, and the ranges of the next row group can be
 * predicted.
 * <p>
 * Chunk boundaries follow parquet-java: a chunk starts at its dictionary page
 * offset when present and smaller than the first data page offset, otherwise
 * at the first data page offset, and spans {@code total_compressed_size}
 * bytes.
 * </p>
 */
final class ParquetFileLayout {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ENCRYPTED_MAGIC = "PARE".getBytes(StandardCharsets.US_ASCII);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int TAIL_SIZE = FOOTER_LENGTH_SIZE + MAGIC.length;

    /** A byte range of the file. */
    record ByteRange(long offset, int length) {

        long end() {
            return offset + length;
        }
    }

    /** A column chunk of a row group. */
    record Chunk(int rowGroup, int column, long offset, long length) {

        long end() {
            return offset + length;
        }
    }

    /** The row group and column positions covered by a set of ranges. */
    record Projection(int rowGroup, BitSet columns) {
    }

    /**
     * Thrown when the file has a structure this class can not or does not want
     * to interpret, such as an encrypted footer.
     */
    static class UnsupportedLayoutException extends IOException {

        private static final long serialVersionUID = 1L;

        UnsupportedLayoutException(String message) {
            super(message);
        }
    }

    private final List<List<Chunk>> rowGroups;
    private final Map<Long, Chunk> chunksByOffset;

    private ParquetFileLayout(List<List<Chunk>> rowGroups, Map<Long, Chunk> chunksByOffset) {
        this.rowGroups = rowGroups;
        this.chunksByOffset = chunksByOffset;
    }

    /**
     * Reads and decodes the footer of the file behind the reader.
     *
     * @param reader source of the file bytes; the footer is read through it, so a
     *               reader with a cached footer serves it from memory
     * @return the decoded layout
     * @throws UnsupportedLayoutException if the footer is encrypted or the
     *                                    structure is not supported
     * @throws IOException                if the file is not a Parquet file or can
     *                                    not be read
     */
    static ParquetFileLayout read(SeekableReader reader) throws IOException {
        long fileLength = reader.getLength();
        if (fileLength < MAGIC.length + TAIL_SIZE) {
            throw new IOException("Not a Parquet file: length " + fileLength + " is too small");
        }
        byte[] tail = new byte[TAIL_SIZE];
        reader.readFully(fileLength - TAIL_SIZE, tail, 0, TAIL_SIZE);

        byte[] magic = Arrays.copyOfRange(tail, FOOTER_LENGTH_SIZE, TAIL_SIZE);
        if (Arrays.equals(ENCRYPTED_MAGIC, magic)) {
            throw new UnsupportedLayoutException("Encrypted footer");
        }
        if (!Arrays.equals(MAGIC, magic)) {
            throw new IOException("Not a Parquet file: unexpected magic " + Arrays.toString(magic));
        }
        long footerLength = readLittleEndianInt(tail) & 0xFFFFFFFFL;
        long footerOffset = fileLength - TAIL_SIZE - footerLength;
        if (footerLength <= 0 || footerLength > Integer.MAX_VALUE || footerOffset < MAGIC.length) {
            throw new IOException("Corrupted Parquet file: footer of " + footerLength + " bytes at " + footerOffset);
        }
        byte[] footer = new byte[(int) footerLength];
        reader.readFully(footerOffset, footer, 0, footer.length);
        FileMetaData metadata = Util.readFileMetaData(new ByteArrayInputStream(footer));
        return of(metadata);
    }

    /**
     * Builds the layout from decoded thrift metadata.
     *
     * @param metadata decoded footer
     * @return the layout
     * @throws UnsupportedLayoutException if some column chunk lacks plain
     *                                    metadata, points to another file or
     *                                    overlaps another chunk
     */
    static ParquetFileLayout of(FileMetaData metadata) throws UnsupportedLayoutException {
        List<RowGroup> groups = metadata.getRow_groups();
        if (groups == null) {
            groups = List.of();
        }
        List<List<Chunk>> rowGroups = new ArrayList<>(groups.size());
        Map<Long, Chunk> chunksByOffset = new HashMap<>();
        for (int rowGroup = 0; rowGroup < groups.size(); rowGroup++) {
            List<ColumnChunk> columns = groups.get(rowGroup).getColumns();
            List<Chunk> chunks = new ArrayList<>(columns == null ? 0 : columns.size());
            if (columns != null) {
                for (int column = 0; column < columns.size(); column++) {
                    Chunk chunk = toChunk(rowGroup, column, columns.get(column));
                    chunks.add(chunk);
                    if (chunk.length() > 0 && chunksByOffset.putIfAbsent(chunk.offset(), chunk) != null) {
                        throw new UnsupportedLayoutException("Column chunks overlap at offset " + chunk.offset());
                    }
                }
            }
            rowGroups.add(List.copyOf(chunks));
        }
        return new ParquetFileLayout(List.copyOf(rowGroups), chunksByOffset);
    }

    private static Chunk toChunk(int rowGroup, int column, ColumnChunk columnChunk) throws UnsupportedLayoutException {
        if (columnChunk.isSetFile_path() && columnChunk.getFile_path() != null) {
            throw new UnsupportedLayoutException("Column chunk stored in external file " + columnChunk.getFile_path());
        }
        ColumnMetaData meta = columnChunk.getMeta_data();
        if (meta == null) {
            throw new UnsupportedLayoutException("Column chunk without plain metadata (encrypted column)");
        }
        long dataPageOffset = meta.getData_page_offset();
        long dictionaryPageOffset = meta.isSetDictionary_page_offset() ? meta.getDictionary_page_offset() : 0;
        long offset = (dictionaryPageOffset > 0 && dictionaryPageOffset < dataPageOffset)
                ? dictionaryPageOffset
                : dataPageOffset;
        return new Chunk(rowGroup, column, offset, meta.getTotal_compressed_size());
    }

    int rowGroupCount() {
        return rowGroups.size();
    }

    /**
     * The chunks of a row group in column order.
     */
    List<Chunk> chunks(int rowGroup) {
        return rowGroups.get(rowGroup);
    }

    /**
     * Resolves which row group and columns a set of requested ranges covers.
     * Every range must be an exact concatenation of whole column chunks of the
     * same row group, as parquet-java requests when no page filtering is
     * applied.
     *
     * @param ranges requested ranges
     * @return the projection, or null when the ranges do not cover whole chunks of
     *         a single row group
     */
    Projection projectionOf(List<ByteRange> ranges) {
        if (ranges.isEmpty()) {
            return null;
        }
        int rowGroup = -1;
        BitSet columns = new BitSet();
        for (ByteRange range : ranges) {
            long position = range.offset();
            long end = range.end();
            while (position < end) {
                Chunk chunk = chunksByOffset.get(position);
                if (chunk == null) {
                    return null;
                }
                if (rowGroup == -1) {
                    rowGroup = chunk.rowGroup();
                } else if (chunk.rowGroup() != rowGroup) {
                    return null;
                }
                columns.set(chunk.column());
                position = chunk.end();
            }
            if (position != end) {
                return null;
            }
        }
        return rowGroup == -1 ? null : new Projection(rowGroup, columns);
    }

    /**
     * Predicts the spans the range reader will physically read for the given
     * columns of a row group. Chunks whose gap is at most {@code mergeGap} bytes
     * are merged into a single span, in file order, exactly as
     * {@link RangeReader} merges the ranges parquet-java requests; the bytes of
     * the gaps are downloaded too and count against {@code maxBytes}. When the
     * total exceeds {@code maxBytes}, the prediction is cut at chunk granularity,
     * so the last predicted span may be a prefix of the span the reader will
     * finally request. Nothing is predicted when a column chunk is too large to
     * be read with vectored IO.
     *
     * @param rowGroup index of the row group to predict
     * @param columns  column positions to include
     * @param maxBytes maximum number of bytes to predict
     * @param mergeGap maximum gap between chunks read as a single span
     * @return the predicted spans, possibly empty
     */
    List<ByteRange> predict(int rowGroup, BitSet columns, long maxBytes, long mergeGap) {
        List<Chunk> chunks = rowGroups.get(rowGroup);
        List<ByteRange> predicted = new ArrayList<>();
        long budget = maxBytes;
        long currentStart = -1;
        long currentEnd = -1;
        for (int column = columns.nextSetBit(0); column >= 0; column = columns.nextSetBit(column + 1)) {
            if (column >= chunks.size()) {
                return List.of();
            }
            Chunk chunk = chunks.get(column);
            if (chunk.length() <= 0) {
                continue;
            }
            if (chunk.length() >= Integer.MAX_VALUE) {
                return List.of();
            }
            boolean mergeable = currentStart >= 0 && chunk.offset() >= currentEnd
                    && chunk.offset() - currentEnd <= mergeGap
                    && (chunk.end() - currentStart) <= Integer.MAX_VALUE;
            long cost = mergeable ? chunk.end() - currentEnd : chunk.length();
            if (cost > budget) {
                break;
            }
            if (mergeable) {
                currentEnd = chunk.end();
            } else {
                addRange(predicted, currentStart, currentEnd);
                currentStart = chunk.offset();
                currentEnd = chunk.end();
            }
            budget -= cost;
        }
        addRange(predicted, currentStart, currentEnd);
        return predicted;
    }

    private static void addRange(List<ByteRange> ranges, long start, long end) {
        if (start >= 0 && end > start) {
            ranges.add(new ByteRange(start, (int) (end - start)));
        }
    }

    private static int readLittleEndianInt(byte[] bytes) {
        return (bytes[0] & 0xFF)
                | ((bytes[1] & 0xFF) << 8)
                | ((bytes[2] & 0xFF) << 16)
                | ((bytes[3] & 0xFF) << 24);
    }

}
