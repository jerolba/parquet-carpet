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
package com.jerolba.carpet.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 *
 * In comparison to the default implementation provided by Apache Parquet
 * HadoopInputFile, this implementation is specific to reading Parquet files
 * from the file system, whereas Apache Parquet provides a more generic
 * implementation that allows reading Parquet files from any data source, not
 * just the file system.
 *
 * Some code is inspired from:
 * https://github.com/benwatson528/intellij-avro-parquet-plugin/blob/master/src/main/java/uk/co
 * \ /hadoopathome/intellij/viewer/fileformat/LocalInputFile.java
 * https://github.com/tideworks/arvo2parquet/blob/master/src/main/java/com/tideworks/data_load/io/InputFile.java
 */

public class FileSystemInputFile implements InputFile {

    private final Path path;

    /**
     *
     * Constructs a FileSystemInputFile with the specified file.
     *
     * @param file the file to read from
     */
    public FileSystemInputFile(File file) {
        this.path = file.toPath();
    }

    /**
     *
     * Constructs a FileSystemInputFile with the specified Path.
     *
     * @param path the path to read from
     */
    public FileSystemInputFile(Path path) {
        this.path = path;
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
        return Files.size(path);
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
        return new SeekableFileInputStream(path);
    }

    private static class SeekableFileInputStream extends SeekableInputStream {

        private final SeekableByteChannel channel;
        private final long fileLength;
        private long markedPos = 0;

        SeekableFileInputStream(Path path) throws IOException {
            this.channel = Files.newByteChannel(path, StandardOpenOption.READ);
            this.fileLength = channel.size();
        }

        public long getLength() throws IOException {
            return fileLength;
        }

        @Override
        public int read() throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                return -1;
            }
            return buffer.get(0) & 0xFF;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return channel.read(ByteBuffer.wrap(b, off, len));
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
            channel.close();
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
            channel.position(markedPos);
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void seek(long l) throws IOException {
            channel.position(l);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            readFully(ByteBuffer.wrap(bytes, start, len));
        }

        @Override
        public int read(ByteBuffer byteBuffer) throws IOException {
            return channel.read(byteBuffer);
        }

        @Override
        public void readFully(ByteBuffer byteBuffer) throws IOException {
            while (byteBuffer.hasRemaining()) {
                int bytesRead = channel.read(byteBuffer);
                if (bytesRead == -1) {
                    throw new IOException("Missing " + byteBuffer.remaining() + " bytes left to read from File");
                }
            }
        }
    }
}
