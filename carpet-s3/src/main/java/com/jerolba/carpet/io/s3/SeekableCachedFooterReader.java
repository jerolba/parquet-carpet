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
import java.util.List;
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
    private boolean closed = false;
    private long pos = 0;

    SeekableCachedFooterReader(SeekableReader seekableReader, Executor executor) throws IOException {
        this.seekableReader = new CachedFooterProxyReader(seekableReader);
        if (executor == null) {
            this.rangeReader = new SequentialReader(this.seekableReader);
        } else {
            this.rangeReader = new VectorReader(this.seekableReader, executor);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
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
        byte[] buf = new byte[1];
        seekableReader.readFully(pos, buf, 0, 1);
        pos += 1;
        return buf[0] & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int start, int len) throws IOException {
        checkOpen();
        logger.debug("read(byte[], int, int) - Current position: {}, Bytes to read: {}", pos, len);
        int readed = seekableReader.readFully(pos, bytes, start, len);
        pos += readed;
        return readed;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        checkOpen();
        int len = byteBuffer.remaining();
        logger.debug("read(ByteBuffer) - Current position: {}, Bytes to read: {}", pos, len);
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

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
        checkOpen();
        rangeReader.readVectored(ranges, allocator);
    }

    @Override
    public boolean readVectoredAvailable(ByteBufferAllocator allocator) {
        return !allocator.isDirect();
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

}
