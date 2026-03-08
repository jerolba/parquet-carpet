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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A proxy reader that caches the footer of a Parquet file to optimize read
 * operations. This class implements the {@link SeekableReader} interface and
 * provides methods to read from the cached footer when the requested position
 * falls within the cached range. If the requested position is outside the
 * cached range, it delegates the read operation to the underlying reader.
 */
class CachedFooterProxyReader implements SeekableReader {

    private static final int DEFAULT_FOOTER_SIZE = 8 * 1024 * 1024; // 8 MiB footer size

    private final SeekableReader reader;
    private long contentLength;
    private final long footerPosition;
    private byte[] footer;

    public CachedFooterProxyReader(SeekableReader reader, int footerSize) throws IOException {
        this.reader = reader;
        this.contentLength = reader.getLength();
        if (contentLength > footerSize) {
            this.footer = new byte[footerSize];
            this.footerPosition = (contentLength - footerSize);
        } else {
            this.footer = new byte[(int) contentLength];
            this.footerPosition = 0;
        }
        reader.readFully(footerPosition, footer, 0, footer.length);
    }

    public CachedFooterProxyReader(SeekableReader reader) throws IOException {
        this(reader, DEFAULT_FOOTER_SIZE);
    }

    @Override
    public long getLength() throws IOException {
        return contentLength;
    }

    @Override
    public int readFully(long pos, byte[] b, int off, int len) throws IOException {
        if (isCached(pos, len)) {
            readBytesFromPos(pos, b, off, len);
            return len;
        }
        return reader.readFully(pos, b, off, len);
    }

    @Override
    public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
        int len = byteBuffer.remaining();
        if (isCached(pos, len)) {
            readBytesFromPos(pos, byteBuffer);
            return len;
        }
        return reader.readFully(pos, byteBuffer);
    }

    @Override
    public void close() throws IOException {
        footer = null;
        contentLength = 0;
        reader.close();
    }

    /**
     * Reads a byte from the cached footer at the given position.
     *
     * @param position the position to read from
     * @return the byte value at the given position
     * @throws UnsupportedOperationException if the position is not cached
     */
    int read(long position) {
        return footer[footerOffset(position)] & 0xFF;
    }

    /**
     * Reads bytes from the cached footer into the provided array.
     *
     * @param pos    the starting position to read from
     * @param toRead the byte array to read into
     * @param start  the starting index in the byte array to write to
     * @param len    the number of bytes to read
     * @throws UnsupportedOperationException if the position is not cached
     * @throws IndexOutOfBoundsException     if the requested length exceeds the
     *                                       available bytes in the footer
     */
    void readBytesFromPos(long pos, byte[] toRead, int start, int len) {
        int footerOffset = footerOffset(pos);
        int remainingInFooter = footer.length - footerOffset;
        if (len > remainingInFooter) {
            throw new IndexOutOfBoundsException(
                    "Requested " + len + " bytes but only " + remainingInFooter + " bytes available in footer");
        }
        if (start + len > toRead.length) {
            throw new IndexOutOfBoundsException(
                    "Destination array overflow: start=" + start + ", len=" + len + ", array length=" + toRead.length);
        }
        System.arraycopy(footer, footerOffset, toRead, start, len);
    }

    /**
     * Reads bytes from the cached footer into the provided ByteBuffer.
     *
     * @param pos        the starting position to read from
     * @param byteBuffer the ByteBuffer to read into
     * @throws UnsupportedOperationException if the position is not cached
     * @throws IndexOutOfBoundsException     if the requested length exceeds the
     *                                       available bytes in the footer
     */
    void readBytesFromPos(long pos, ByteBuffer byteBuffer) {
        int footerOffset = footerOffset(pos);
        int remainingInFooter = footer.length - footerOffset;
        int len = byteBuffer.remaining();
        if (len > remainingInFooter) {
            throw new IndexOutOfBoundsException(
                    "Requested " + len + " bytes but only " + remainingInFooter
                            + " bytes available in footer");
        }
        byteBuffer.put(footer, footerOffset, len);
    }

    /**
     * Checks if the given range is within the cached footer range.
     *
     * @param fromPosition the position to check
     * @param length       the number of bytes to check
     * @return true if the range is within the cached footer range, false otherwise
     */
    boolean isCached(long fromPosition, int length) {
        if (footer == null || fromPosition < 0 || fromPosition + length > contentLength) {
            return false;
        }
        return fromPosition >= footerPosition;
    }

    /**
     * Checks if the given position is within the cached footer range.
     *
     * @param fromPosition the position to check
     * @return true if the position is within the cached footer range, false
     *         otherwise
     */
    boolean isCached(long fromPosition) {
        return isCached(fromPosition, 0);
    }

    /**
     * Calculates the offset within the footer for the given position.
     *
     * @param pos the position to calculate the offset for
     * @return the offset within the footer
     */
    private int footerOffset(long pos) {
        if (!isCached(pos)) {
            throw new UnsupportedOperationException("Position " + pos + " is not cached.");
        }
        return (int) (pos - footerPosition);
    }

}