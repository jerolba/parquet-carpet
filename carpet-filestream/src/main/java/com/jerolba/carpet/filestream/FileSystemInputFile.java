/**
 * Copyright 2022 Jerónimo López Bezanilla
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
package com.jerolba.carpet.filestream;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 *
 * In comparison to the default implementation provided by Apache Parquet
 * {@link HadoopInputFile}, this implementation is specific to reading Parquet
 * files from the file system, whereas Apache Parquet provides a more generic
 * implementation that allows reading Parquet files from any data source, not
 * just the file system.
 *
 * Some code is inspired from:
 * https://github.com/benwatson528/intellij-avro-parquet-plugin/blob/master/src/main/java/uk/co
 * \ /hadoopathome/intellij/viewer/fileformat/LocalInputFile.java
 * https://github.com/tideworks/arvo2parquet/blob/master/src/main/java/com/tideworks/data_load/io/InputFile.java
 */

//Review usage of DelegatingSeekableInputStream
public class FileSystemInputFile implements InputFile {

    private final File file;

    /**
     *
     * Constructs a FileSystemInputFile with the specified file.
     *
     * @param file the file to read from
     */
    public FileSystemInputFile(File file) {
        this.file = file;
    }

    /**
     *
     * Returns the length of the file.
     *
     * @return the length of the file
     * @throws IOException if an error occurs while getting the length of the file
     */
    @Override
    public long getLength() throws IOException {
        return file.length();
    }

    /**
     *
     * Creates a new stream for reading from the file.
     *
     * @return a new SeekableInputStream for reading from the file
     * @throws IOException if an error occurs while creating the stream
     */
    @Override
    public SeekableInputStream newStream() throws IOException {
        return new SeekableFileInputStream(file);
    }

    private static class SeekableFileInputStream extends SeekableInputStream {

        private static final int COPY_SIZE = 16 * 1024;
        private final RandomAccessFile file;
        private final byte[] copyBuffer = new byte[COPY_SIZE];
        private long markedPos = 0;

        SeekableFileInputStream(File file) throws IOException {
            this.file = new RandomAccessFile(file, "r");
        }

        public long getLength() throws IOException {
            return file.length();
        }

        @Override
        public int read() throws IOException {
            return file.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return file.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return file.read(b, off, len);
        }

        @Override
        public long skip(long skipCount) throws IOException {
            if (skipCount <= 0) {
                return 0;
            }
            long currentOffset = getPos();
            long fileLenght = getLength();
            if (currentOffset >= fileLenght) {
                return 0;
            }
            long newPos = Math.min(currentOffset + skipCount, fileLenght);
            seek(newPos);
            long curPos = getPos();
            assert curPos == newPos;
            return curPos - currentOffset;
        }

        @Override
        public int available() throws IOException {
            long remaining = getLength() - getPos();
            if (remaining > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            return (int) remaining;
        }

        @Override
        public void close() throws IOException {
            file.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            try {
                markedPos = getPos();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public synchronized void reset() throws IOException {
            file.seek(markedPos);
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public long getPos() throws IOException {
            return file.getFilePointer();
        }

        @Override
        public void seek(long l) throws IOException {
            file.seek(l);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            file.readFully(bytes);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            file.readFully(bytes, start, len);
        }

        @Override
        public int read(ByteBuffer byteBuffer) throws IOException {
            int bytesToRead = getBytesToRead(byteBuffer);
            int totalBytesReaded = 0;
            int readedBytes;

            while ((readedBytes = file.read(copyBuffer, 0, bytesToRead)) == COPY_SIZE) {
                totalBytesReaded += readedBytes;
                byteBuffer.put(copyBuffer);
                bytesToRead = getBytesToRead(byteBuffer);
            }

            if (readedBytes < 0) {
                return totalBytesReaded == 0 ? -1 : totalBytesReaded;
            }
            byteBuffer.put(copyBuffer, 0, readedBytes);
            return totalBytesReaded + readedBytes;
        }

        @Override
        public void readFully(ByteBuffer byteBuffer) throws IOException {
            int bytesToRead = getBytesToRead(byteBuffer);
            int readedBytes = 0;

            while (bytesToRead > 0 && (readedBytes = file.read(copyBuffer, 0, bytesToRead)) >= 0) {
                byteBuffer.put(copyBuffer, 0, readedBytes);
                bytesToRead = getBytesToRead(byteBuffer);
            }
            if (readedBytes < 0 && byteBuffer.remaining() > 0) {
                throw new IOException("Missing " + byteBuffer.remaining() + " bytes left to read from File");
            }
        }

        private int getBytesToRead(ByteBuffer byteBufr) {
            return Math.min(byteBufr.remaining(), COPY_SIZE);
        }
    }
}
