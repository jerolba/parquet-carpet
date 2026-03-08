package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.nio.ByteBuffer;

class CachedFooter {

    private static final int DEFAULT_FOOTER_SIZE = 8 * 1024 * 1024; // 8 MiB footer size

    private final long contentLength;
    private final long footerPosition;
    private final byte[] footer;

    public CachedFooter(SeekableReader reader, int footerSize) throws IOException {
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

    public CachedFooter(SeekableReader reader) throws IOException {
        this(reader, DEFAULT_FOOTER_SIZE);
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
     * Checks if the given position is within the cached footer range.
     *
     * @param fromPosition the position to check
     * @return true if the position is within the cached footer range, false
     *         otherwise
     */
    boolean isCached(long fromPosition) {
        if (fromPosition < 0 || fromPosition >= contentLength) {
            return false;
        }
        return fromPosition >= footerPosition;
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