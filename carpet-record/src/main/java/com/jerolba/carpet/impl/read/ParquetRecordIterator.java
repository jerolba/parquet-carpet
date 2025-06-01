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
package com.jerolba.carpet.impl.read;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;

import org.apache.parquet.hadoop.ParquetReader;

import com.jerolba.carpet.CloseableIterator;

/**
 * This class provides an iterator for the records in a Parquet file.
 *
 * @param <T> the type of the records in the Parquet file
 */
public class ParquetRecordIterator<T> implements CloseableIterator<T> {

    private final ParquetReader<T> reader;
    private T nextRecord;

    /**
     * Creates a new {@code ParquetRecordIterator} instance from the specified
     * record class and reader.
     *
     * @param recordClass the class of the records in the Parquet file
     * @param reader      the reader for the Parquet data
     * @throws IOException if an I/O error occurs
     */
    public ParquetRecordIterator(ParquetReader<T> reader) throws IOException {
        this.reader = reader;
        this.nextRecord = reader.read();
    }

    /**
     * Returns {@code true} if the iteration has more records.
     *
     * @return {@code true} if the iteration has more records, {@code false}
     *         otherwise
     */
    @Override
    public boolean hasNext() {
        return nextRecord != null;
    }

    /**
     * Returns the next record in the iteration.
     *
     * @return the next record in the iteration
     * @throws NoSuchElementException if the iteration has no more records
     */
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T res = nextRecord;
        try {
            this.nextRecord = reader.read();
            if (nextRecord == null) {
                uncheckedCloseReader();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return res;
    }

    /**
     * Closes the underlying {@link ParquetReader} and releases any system resources
     * associated with it.
     * <p>
     * If the iterator is fully consumed, this method is called automatically.
     * However, if iteration is stopped before reaching the end, you must call
     * {@code close()} manually to avoid resource leaks.
     * <p>
     * After calling this method, further calls to {@code hasNext()} or
     * {@code next()} may throw an exception.
     *
     * @throws IOException if an I/O error occurs while closing the reader
     */
    @Override
    public void close() throws IOException {
        reader.close();
        nextRecord = null;
    }

    /**
     * Closes the underlying reader and releases any resources associated with it,
     * suppressing any checked exceptions.
     */
    public void uncheckedCloseReader() {
        try {
            close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}