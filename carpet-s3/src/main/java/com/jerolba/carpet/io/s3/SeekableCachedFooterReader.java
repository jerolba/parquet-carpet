package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SeekableCachedFooterReader extends SeekableInputStream {

    private static final Logger logger = LoggerFactory.getLogger(SeekableCachedFooterReader.class);

    private final SeekableReader seekableReader;
    private final RangeReader rangeReader;
    private CachedFooter cachedFooter;
    private boolean closed = false;
    private long pos = 0;

    SeekableCachedFooterReader(SeekableReader seekableReader, Executor executor) throws IOException {
        this.seekableReader = seekableReader;
        this.cachedFooter = new CachedFooter(seekableReader);
        if (executor == null) {
            this.rangeReader = new SequentialReader(seekableReader);
        } else {
            this.rangeReader = new VectorReader(seekableReader, executor);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            cachedFooter = null;
            rangeReader.close();
            seekableReader.close();
        }
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        checkOpen();
        if (pos < 0) {
            throw new IOException("Invalid seek position: " + pos);
        }
        logger.debug("seek() - Seeking to position: {}", pos);
        this.pos = pos;
    }

    @Override
    public int read() throws IOException {
        checkOpen();
        logger.debug("read() - Reading at position: {}", pos);
        if (cachedFooter.isCached(pos)) {
            int returnValue = cachedFooter.read(pos);
            pos += 1;
            return returnValue;
        }
        byte[] buf = new byte[1];
        seekableReader.readFully(pos, buf, 0, 1);
        pos += 1;
        return buf[0] & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int start, int len) throws IOException {
        checkOpen();
        logger.debug("read(byte[], int, int) - Current position: {}, Bytes to read: {}", pos, len);
        if (cachedFooter.isCached(pos)) {
            cachedFooter.readBytesFromPos(pos, bytes, start, len);
            pos += len;
            return len;
        }
        int readed = seekableReader.readFully(pos, bytes, start, len);
        pos += readed;
        return readed;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        checkOpen();
        int len = byteBuffer.remaining();
        logger.debug("read(ByteBuffer) - Current position: {}, Bytes to read: {}", pos, len);
        if (cachedFooter.isCached(pos)) {
            cachedFooter.readBytesFromPos(pos, byteBuffer);
            pos += len;
            return len;
        }
        int readed = seekableReader.readFully(pos, byteBuffer);
        pos += readed;
        return readed;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        read(bytes, start, len);
    }

    @Override
    public void readFully(ByteBuffer byteBuffer) throws IOException {
        read(byteBuffer);
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
        checkOpen();
        List<ParquetFileRange> uncachedRanges = resolveCachedRanges(ranges, allocator);
        if (!uncachedRanges.isEmpty()) {
            rangeReader.readVectored(uncachedRanges, allocator);
        }
    }

    private List<ParquetFileRange> resolveCachedRanges(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) {
        List<ParquetFileRange> uncachedRanges = new ArrayList<>();
        for (ParquetFileRange range : ranges) {
            long offset = range.getOffset();
            if (cachedFooter.isCached(offset)) {
                int length = range.getLength();
                ByteBuffer byteBuffer = allocator.allocate(length);
                cachedFooter.readBytesFromPos(offset, byteBuffer);
                byteBuffer.flip();
                range.setDataReadFuture(CompletableFuture.completedFuture(byteBuffer));
            } else {
                uncachedRanges.add(range);
            }
        }
        return uncachedRanges;
    }

    @Override
    public boolean readVectoredAvailable(final ByteBufferAllocator allocator) {
        return true;
    }

}
