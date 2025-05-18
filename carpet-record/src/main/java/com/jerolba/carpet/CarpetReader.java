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

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import com.jerolba.carpet.io.FileSystemInputFile;

/**
 * CarpetReader wraps the creation of CarpetParquetReader CarpetReader is an
 * Iterable and Streamable class that can be used to read a Parquet file and
 * iterate over its records.
 *
 * @param <T> The type of the records being read.
 */
public class CarpetReader<T> implements Iterable<T> {

    private final Class<T> recordClass;
    private Builder<T> builder;

    /**
     *
     * Creates a new {@code CarpetReader} instance from the specified InputFile and
     * record class.
     *
     * @param inputFile   the input file from which the records will be read
     * @param recordClass the class of the records being read
     */
    public CarpetReader(InputFile inputFile, Class<T> recordClass) {
        this.builder = new Builder<>(inputFile, recordClass);
        this.recordClass = recordClass;
    }

    /**
     *
     * Creates a new {@code CarpetReader} instance from the specified File and
     * record class.
     *
     * @param file        the File containing the Parquet data
     * @param recordClass the class of the records being read
     */
    public CarpetReader(File file, Class<T> recordClass) {
        this(new FileSystemInputFile(file), recordClass);
    }

    private CarpetReader(Builder<T> builder, Class<T> recordClass) {
        this.recordClass = recordClass;
        this.builder = builder;
    }

    /**
     * Feature that determines whether encountering of missed parquet column should
     * result in a failure (by throwing a RecordTypeConversionException) or not.
     *
     * Feature is enabled by default.
     *
     * @param value The new value for the failOnMissingColumn flag.
     * @return a new instance of CarpetReader
     */
    public CarpetReader<T> withFailOnMissingColumn(boolean value) {
        builder.failOnMissingColumn(value);
        return this;
    }

    /**
     * Feature that determines whether encountering null is an error when
     * deserializing into Java primitive types (like 'int' or 'double'). If it is, a
     * RecordTypeConversionException is thrown to indicate this; if not, default
     * value is used (0 for 'int', 0.0 for double, same defaulting as what JVM
     * uses).
     *
     * Feature is disabled by default.
     *
     * @param value The new value for the failOnNullForPrimitives flag.
     * @return a new instance of CarpetReader
     */
    public CarpetReader<T> withFailOnNullForPrimitives(boolean value) {
        builder.failOnNullForPrimitives(value);
        return this;
    }

    /**
     * Feature that determines whether coercion from one number type to other number
     * type with less resolutions is allowed or not. If disabled, coercion truncates
     * value.
     *
     * A narrowing primitive conversion may lose information about the overall
     * magnitude of a numeric value and may also lose precision and range. Narrowing
     * follows
     * <a href="https://docs.oracle.com/javase/specs/jls/se10/html/jls-5.html">Java
     * Language Specification</a>
     *
     * Feature is disabled by default.
     *
     * @param value
     * @return a new instance of CarpetReader
     */
    public CarpetReader<T> withFailNarrowingPrimitiveConversion(boolean value) {
        builder.failNarrowingPrimitiveConversion(value);
        return this;
    }

    public CarpetReader<T> withFieldMatchingStrategy(FieldMatchingStrategy value) {
        builder.fieldMatchingStrategy(value);
        return this;
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
    public CloseableIterator<T> iterator() {
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
            return new RecordIterator<>(recordClass, builder.buildParquetReader());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * This class provides an iterator for the records in a Parquet file.
     *
     * @param <T> the type of the records in the Parquet file
     */
    private static class RecordIterator<T> implements CloseableIterator<T> {

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

    public static class Builder<T> {

        private final CarpetParquetReader.Builder<T> builder;
        private final Class<T> recordClass;

        /**
         *
         * Creates a new {@code Builder} instance from the specified InputFile and
         * record class.
         *
         * @param inputFile   the input file from which the records will be read
         * @param recordClass the class of the records being read
         */
        public Builder(InputFile file, Class<T> readClass) {
            this.builder = CarpetParquetReader.builder(file, readClass);
            this.recordClass = readClass;
        }

        /**
         *
         * Creates a new {@code Builder} instance from the specified File and record
         * class.
         *
         * @param file        the File containing the Parquet data
         * @param recordClass the class of the records being read
         */
        public Builder(File file, Class<T> recordClass) {
            this(new FileSystemInputFile(file), recordClass);
        }

        /**
         * Feature that determines whether encountering of missed parquet column should
         * result in a failure (by throwing a RecordTypeConversionException) or not.
         *
         * Feature is enabled by default.
         *
         * @param failOnMissingColumn
         * @return Carpet Reader Builder
         */
        public Builder<T> failOnMissingColumn(boolean failOnMissingColumn) {
            this.builder.failOnMissingColumn(failOnMissingColumn);
            return this;
        }

        /**
         * Feature that determines whether encountering null is an error when
         * deserializing into Java primitive types (like 'int' or 'double'). If it is, a
         * RecordTypeConversionException is thrown to indicate this; if not, default
         * value is used (0 for 'int', 0.0 for double, same defaulting as what JVM
         * uses).
         *
         * Feature is disabled by default.
         *
         * @param failOnNullForPrimitives
         * @return Carpet Reader Builder
         */
        public Builder<T> failOnNullForPrimitives(boolean failOnNullForPrimitives) {
            this.builder.failOnNullForPrimitives(failOnNullForPrimitives);
            return this;
        }

        /**
         * Feature that determines whether coercion from one number type to other number
         * type with less resolutions is allowed or not. If disabled, coercion truncates
         * value.
         *
         * A narrowing primitive conversion may lose information about the overall
         * magnitude of a numeric value and may also lose precision and range. Narrowing
         * follows
         * <a href="https://docs.oracle.com/javase/specs/jls/se10/html/jls-5.html">Java
         * Language Specification</a>
         *
         * Feature is disabled by default.
         *
         * @param failNarrowingPrimitiveConversion
         * @return Carpet Reader Builder
         */
        public Builder<T> failNarrowingPrimitiveConversion(boolean failNarrowingPrimitiveConversion) {
            this.builder.failNarrowingPrimitiveConversion(failNarrowingPrimitiveConversion);
            return this;
        }

        public Builder<T> fieldMatchingStrategy(FieldMatchingStrategy fieldMatchingStrategy) {
            this.builder.fieldMatchingStrategy(fieldMatchingStrategy);
            return this;
        }

        public Builder<T> withConf(ParquetConfiguration conf) {
            this.builder.withConf(conf);
            return this;
        }

        public Builder<T> withFilter(Filter filter) {
            this.builder.withFilter(filter);
            return this;
        }

        public Builder<T> withAllocator(ByteBufferAllocator allocator) {
            this.builder.withAllocator(allocator);
            return this;
        }

        public Builder<T> useSignedStringMinMax(boolean useSignedStringMinMax) {
            this.builder.useSignedStringMinMax(useSignedStringMinMax);
            return this;
        }

        public Builder<T> useSignedStringMinMax() {
            this.builder.useSignedStringMinMax();
            return this;
        }

        public Builder<T> useStatsFilter(boolean useStatsFilter) {
            this.builder.useStatsFilter(useStatsFilter);
            return this;
        }

        public Builder<T> useStatsFilter() {
            this.builder.useStatsFilter();
            return this;
        }

        public Builder<T> useDictionaryFilter(boolean useDictionaryFilter) {
            this.builder.useDictionaryFilter(useDictionaryFilter);
            return this;
        }

        public Builder<T> useDictionaryFilter() {
            this.builder.useDictionaryFilter();
            return this;
        }

        public Builder<T> useRecordFilter(boolean useRecordFilter) {
            this.builder.useRecordFilter(useRecordFilter);
            return this;
        }

        public Builder<T> useRecordFilter() {
            this.builder.useRecordFilter();
            return this;
        }

        public Builder<T> useColumnIndexFilter(boolean useColumnIndexFilter) {
            this.builder.useColumnIndexFilter(useColumnIndexFilter);
            return this;
        }

        public Builder<T> useColumnIndexFilter() {
            this.builder.useColumnIndexFilter();
            return this;
        }

        public Builder<T> usePageChecksumVerification(boolean usePageChecksumVerification) {
            this.builder.usePageChecksumVerification(usePageChecksumVerification);
            return this;
        }

        public Builder<T> useBloomFilter(boolean useBloomFilter) {
            this.builder.useBloomFilter(useBloomFilter);
            return this;
        }

        public Builder<T> useBloomFilter() {
            this.builder.useBloomFilter();
            return this;
        }

        public Builder<T> usePageChecksumVerification() {
            this.builder.usePageChecksumVerification();
            return this;
        }

        public Builder<T> withFileRange(long start, long end) {
            this.builder.withFileRange(start, end);
            return this;
        }

        public Builder<T> withCodecFactory(CompressionCodecFactory codecFactory) {
            this.builder.withCodecFactory(codecFactory);
            return this;
        }

        public Builder<T> withDecryption(FileDecryptionProperties fileDecryptionProperties) {
            this.builder.withDecryption(fileDecryptionProperties);
            return this;
        }

        public Builder<T> set(String key, String value) {
            this.builder.set(key, value);
            return this;
        }

        private ParquetReader<T> buildParquetReader() throws IOException {
            return this.builder.build();
        }

        public CarpetReader<T> build() {
            return new CarpetReader<>(this, recordClass);
        }

    }

}
