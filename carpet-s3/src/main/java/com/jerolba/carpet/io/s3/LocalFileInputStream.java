package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.parquet.io.SeekableInputStream;

/**
 * Reader for local files downaloaded from S3 when carpet.s3.predownload.file is
 * set to true. It's a copy of
 * com.jerolba.carpet.io.FileSystemInputFile.SeekableFileInputStream to avoid
 * depending on Carpet and make carpet-s3 module independent of carpet-core.
 */
class LocalFileInputStream extends SeekableInputStream {

    private final SeekableByteChannel channel;
    private final long fileLength;
    private long markedPos = 0;

    public LocalFileInputStream(Path path) throws IOException {
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
