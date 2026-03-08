package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;

class SequentialReader implements RangeReader {

    private final SeekableReader seekableReader;

    SequentialReader(SeekableReader seekableReader) {
        this.seekableReader = seekableReader;
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
        for (ParquetFileRange range : ranges) {
            long offset = range.getOffset();
            int length = range.getLength();
            ByteBuffer byteBuffer = allocator.allocate(length);
            try {
                seekableReader.readFully(offset, byteBuffer);
                byteBuffer.flip();
            } catch (IOException e) {
                allocator.release(byteBuffer);
                throw e;
            }
            range.setDataReadFuture(CompletableFuture.completedFuture(byteBuffer));
        }
    }

    @Override
    public void close() {
    }

}
