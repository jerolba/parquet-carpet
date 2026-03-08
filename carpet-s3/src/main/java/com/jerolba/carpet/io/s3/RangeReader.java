package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.util.List;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;

interface RangeReader extends AutoCloseable {

    void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException;

    @Override
    void close();

}
