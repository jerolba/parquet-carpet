package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for a seekable reader that allows reading data from a specific
 * position.
 *
 * This interface provides methods to get the length of the data, read data
 * fully into a byte array or a ByteBuffer, and close the reader when done.
 *
 * <p>
 * Only {@code readFully} methods are provided — there is no plain
 * {@code read(...)} variant. This is intentional: implementations are required
 * to guarantee that all requested bytes are delivered before returning,
 * blocking as long as necessary until the full amount has been read. A
 * partial-read method would force every caller to implement its own retry/loop
 * logic to handle short reads, which adds unnecessary complexity. By exposing
 * only {@code readFully}, the contract is clear: either all requested bytes are
 * read, or an {@link IOException} is thrown.
 *
 * <p>
 * Internal Parquet implementation request always for range of bytes, and
 * doesn't stream the content of a file from a position. This means that the
 * contract of this interface is to read a range of bytes, and not to read a
 * stream of bytes from a position.
 *
 * <p>
 * This interface is intended to be implemented by classes that provide seekable
 * reading capabilities, such as reading from a file, a network stream, s3 or
 * any other data source that supports random access. Implementations must
 * handle the necessary logic to read data from the specified position, retrying
 * internally if the underlying source returns fewer bytes than requested, and
 * manage resources appropriately.
 */

interface SeekableReader extends AutoCloseable {

    long getLength() throws IOException;

    int readFully(long pos, byte[] b, int off, int len) throws IOException;

    int readFully(long pos, ByteBuffer byteBuffer) throws IOException;

    @Override
    void close() throws IOException;

}
