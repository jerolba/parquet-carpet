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

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;

class SeekableReaderHelper {

    /**
     * In-memory SeekableReader backed by a byte array. Thread-safe for concurrent
     * reads since operations only read from the backing array.
     */
    static SeekableReader readerOf(byte[] data) {
        return new SeekableReader() {
            @Override
            public long getLength() {
                return data.length;
            }

            @Override
            public int readFully(long pos, byte[] b, int off, int len) {
                System.arraycopy(data, (int) pos, b, off, len);
                return len;
            }

            @Override
            public int readFully(long pos, ByteBuffer byteBuffer) {
                int len = byteBuffer.remaining();
                byteBuffer.put(data, (int) pos, len);
                return len;
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Virtual SeekableReader that reports a large file size without allocating
     * memory. Data at position {@code pos} is {@code (byte)(pos % 256)}.
     * Thread-safe since computation is stateless.
     */
    static SeekableReader largeVirtualReader(long size) {
        return new SeekableReader() {
            @Override
            public long getLength() {
                return size;
            }

            @Override
            public int readFully(long pos, byte[] b, int off, int len) {
                for (int i = 0; i < len; i++) {
                    b[off + i] = (byte) ((pos + i) % 256);
                }
                return len;
            }

            @Override
            public int readFully(long pos, ByteBuffer byteBuffer) {
                int len = byteBuffer.remaining();
                for (int i = 0; i < len; i++) {
                    byteBuffer.put((byte) ((pos + i) % 256));
                }
                return len;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    static byte[] sequentialBytes(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    static ByteBufferAllocator heapAllocator() {
        return HeapByteBufferAllocator.getInstance();
    }
}
