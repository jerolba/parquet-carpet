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
package com.jerolba.carpet;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import com.jerolba.carpet.io.FileSystemInputFile;

public class CarpetReader<T> implements Iterable<T> {

    private final InputFile inputFile;
    private final Class<T> recordClass;

    private boolean failOnMissingColumn = CarpetParquetReader.DEFAULT_FAIL_ON_MISSING_COLUMN;
    private boolean failOnNullForPrimitives = CarpetParquetReader.DEFAULT_FAIL_ON_NULL_FOR_PRIMITIVES;

    /**
     *
     * Creates a new {@code CarpetReader} instance from the specified input file and
     * record class.
     *
     * @param inputFile   the input file containing the Parquet data
     * @param recordClass the class of the records in the Parquet file
     * @throws IOException if an I/O error occurs
     */
    public CarpetReader(InputFile inputFile, Class<T> recordClass) {
        this.inputFile = inputFile;
        this.recordClass = recordClass;
    }

    /**
     *
     * Creates a new {@code CarpetReader} instance from the specified input file and
     * record class.
     *
     * @param inputFile   the input File containing the Parquet data
     * @param recordClass the class of the records in the Parquet file
     * @throws IOException if an I/O error occurs
     */
    public CarpetReader(File inputFile, Class<T> recordClass) {
        this(new FileSystemInputFile(inputFile), recordClass);
    }

    public CarpetReader<T> withFailOnMissingColumn(boolean value) {
        CarpetReader<T> newInstance = cloneInstance();
        newInstance.failOnMissingColumn = value;
        return newInstance;
    }

    public CarpetReader<T> withFailOnNullForPrimitives(boolean value) {
        CarpetReader<T> newInstance = cloneInstance();
        newInstance.failOnNullForPrimitives = value;
        return newInstance;
    }

    private CarpetReader<T> cloneInstance() {
        CarpetReader<T> newInstace = new CarpetReader<>(inputFile, recordClass);
        newInstace.failOnMissingColumn = failOnMissingColumn;
        newInstace.failOnNullForPrimitives = failOnNullForPrimitives;
        return newInstace;
    }

    /**
     *
     * Returns an {@link Iterator} that can be used to iterate over the records in
     * the Parquet file.
     *
     * @return an iterator for the records in the Parquet file
     * @throws UncheckedIOException if an I/O error occurs
     */
    @Override
    public Iterator<T> iterator() {
        return buildIterator();
    }

    /**
     *
     * Returns a {@link Stream} that can be used to access the records in the
     * Parquet file.
     *
     * @return a stream for the records in the Parquet file
     * @throws IOException if an I/O error occurs
     */
    public Stream<T> stream() {
        RecordIterator<T> iterator = buildIterator();
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
        return StreamSupport.stream(spliterator, false)
                .onClose(iterator::uncheckedCloseReader);
    }

    /**
     *
     * Returns a {@link List} containing all records in the Parquet file.
     *
     * @return a list of all records in the Parquet file
     * @throws IOException if an I/O error occurs
     */
    public List<T> toList() throws IOException {
        List<T> result = new ArrayList<>();
        try (var iterator = buildIterator()) {
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
            return result;
        }
    }

    private RecordIterator<T> buildIterator() {
        try {
            ParquetReader<T> reader = CarpetParquetReader
                    .builder(inputFile, recordClass)
                    .failOnMissingColumn(failOnMissingColumn)
                    .failOnNullForPrimitives(failOnNullForPrimitives)
                    .build();
            return new RecordIterator<>(recordClass, reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * This class provides an iterator for the records in a Parquet file.
     *
     * @param <T> the type of the records in the Parquet file
     */
    private class RecordIterator<T> implements Iterator<T>, Closeable {

        private final ParquetReader<T> reader;
        private T nextRecord;

        /**
         * Creates a new {@code RecordIterator} instance from the specified record class
         * and reader.
         *
         * @param recordClass the class of the records in the Parquet file
         * @param reader      the reader for the Parquet data
         * @throws IOException if an I/O error occurs
         */
        RecordIterator(Class<T> recordClass, ParquetReader<T> reader) throws IOException {
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
         * Closes the underlying reader.
         *
         * @throws IOException if an I/O error occurs
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
        private void uncheckedCloseReader() {
            try {
                close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

    }

}
