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
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import com.jerolba.carpet.io.OutputStreamOutputFile;
import com.jerolba.carpet.model.WriteRecordModelType;

/**
 * A Parquet file writer for writing java records of type T to a file or output
 * stream. The writer is also a consumer that can be used with Java 8 streams.
 *
 * Creates a ParquetWriter with this configuration:
 *
 * <pre>{@code
 * .withWriteMode(Mode.OVERWRITE)
 * .withCompressionCodec(CompressionCodecName.SNAPPY)
 *
 * }</pre>
 *
 * @param <T> The type of records to write.
 */
public class CarpetWriter<T> implements Closeable, Consumer<T> {

    private final ParquetWriter<T> writer;

    /**
     * Constructs a CarpetWriter that writes records of type T to the specified
     * OutputFile.
     *
     * @param outputFile  The output file to write to.
     * @param recordClass The class of the records to write.
     * @throws IOException If an I/O error occurs while creating the Parquet writer.
     */
    public CarpetWriter(OutputFile outputFile, Class<T> recordClass) throws IOException {
        this.writer = new Builder<>(outputFile, recordClass).buildWriter();
    }

    /**
     * Constructs a CarpetWriter that writes records of type T to the specified
     * OutputStream.
     *
     * @param outputStream An OutputStream to write to, of any type.
     * @param recordClass  The class of the records to write.
     * @throws IOException If an I/O error occurs while creating the Parquet writer.
     */
    public CarpetWriter(OutputStream outputStream, Class<T> recordClass) throws IOException {
        OutputStreamOutputFile wrappedStream = new OutputStreamOutputFile(outputStream);
        this.writer = new Builder<>(wrappedStream, recordClass).buildWriter();
    }

    private CarpetWriter(ParquetWriter<T> writer) {
        this.writer = writer;
    }

    /**
     *
     * Writes the specified collection of Java objects to a Parquet file.
     *
     * @param collection the collection of objects to write
     * @throws IOException if an error occurs while writing the records
     */
    public void write(Collection<T> collection) throws IOException {
        for (var value : collection) {
            writer.write(value);
        }
    }

    /**
     *
     * Writes the specified Java object to a Parquet file
     *
     * @param value object to write
     * @throws IOException if an error occurs while writing the records
     */
    public void write(T value) throws IOException {
        writer.write(value);
    }

    /**
     *
     * Writes the specified Java object to a Parquet file implementing Consumer<T>
     *
     * @param value object to write
     * @throws UncheckedIOException if an error occurs while writing the records
     */
    @Override
    public void accept(T value) {
        try {
            writer.write(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     *
     * Writes the specified stream of Java objects to a Parquet file.
     *
     * @param stream the stream of objects to write
     *
     * @throws IOException if an error occurs while writing the records
     */
    public void write(Stream<T> stream) throws IOException {
        Iterator<T> it = stream.iterator();
        while (it.hasNext()) {
            writer.write(it.next());
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    public static class Builder<T> {

        private final CarpetParquetWriter.Builder<T> builder;
        private boolean parquetConfProvided = false;
        private boolean hadoopConfProvided = false;

        public Builder(OutputFile path, Class<T> recordClass) {
            builder = CarpetParquetWriter.builder(path, recordClass)
                    .withWriteMode(Mode.OVERWRITE)
                    .withCompressionCodec(CompressionCodecName.SNAPPY);
        }

        public Builder(OutputStream outputStream, Class<T> recordClass) {
            this(new OutputStreamOutputFile(outputStream), recordClass);
        }

        /**
         * Set the {@link Configuration} used by the constructed writer.
         *
         * @param conf a {@code Configuration}
         * @return this builder for method chaining.
         */
        public Builder<T> withConf(Configuration conf) {
            hadoopConfProvided = true;
            builder.withConf(conf);
            return this;
        }

        /**
         * Set the {@link ParquetConfiguration} used by the constructed writer.
         *
         * @param conf a {@code ParquetConfiguration}
         * @return this builder for method chaining.
         */
        public Builder<T> withConf(ParquetConfiguration conf) {
            parquetConfProvided = true;
            builder.withConf(conf);
            return this;
        }

        /**
         * Set the {@link ParquetFileWriter.Mode write mode} used when creating the
         * backing file for this writer.
         *
         * @param mode a {@code ParquetFileWriter.Mode}
         * @return this builder for method chaining.
         */
        public Builder<T> withWriteMode(ParquetFileWriter.Mode mode) {
            builder.withWriteMode(mode);
            return this;
        }

        /**
         * Set the {@link CompressionCodecName compression codec} used by the
         * constructed writer.
         *
         * @param codecName a {@code CompressionCodecName}
         * @return this builder for method chaining.
         */
        public Builder<T> withCompressionCodec(CompressionCodecName codecName) {
            builder.withCompressionCodec(codecName);
            return this;
        }

        /**
         * Set the {@link CompressionCodecFactory codec factory} used by the constructed
         * writer.
         *
         * @param codecFactory a {@link CompressionCodecFactory}
         * @return this builder for method chaining.
         */
        public Builder<T> withCodecFactory(CompressionCodecFactory codecFactory) {
            builder.withCodecFactory(codecFactory);
            return this;
        }

        /**
         * Set the {@link FileEncryptionProperties file encryption properties} used by
         * the constructed writer.
         *
         * @param encryptionProperties a {@code FileEncryptionProperties}
         * @return this builder for method chaining.
         */
        public Builder<T> withEncryption(FileEncryptionProperties encryptionProperties) {
            builder.withEncryption(encryptionProperties);
            return this;
        }

        /**
         * Set the Parquet format row group size used by the constructed writer.
         *
         * @param rowGroupSize an integer size in bytes
         * @return this builder for method chaining.
         */
        public Builder<T> withRowGroupSize(long rowGroupSize) {
            builder.withRowGroupSize(rowGroupSize);
            return this;
        }

        /**
         * Set the maximum amount of padding, in bytes, that will be used to align row
         * groups with blocks in the underlying filesystem. If the underlying filesystem
         * is not a block filesystem like HDFS, this has no effect.
         *
         * @param maxPaddingSize an integer size in bytes
         * @return this builder for method chaining.
         */
        public Builder<T> withMaxPaddingSize(int maxPaddingSize) {
            builder.withMaxPaddingSize(maxPaddingSize);
            return this;
        }

        /**
         * Enables validation for the constructed writer.
         *
         * @return this builder for method chaining.
         */
        public Builder<T> enableValidation() {
            builder.enableValidation();
            return this;
        }

        /**
         * Enable or disable validation for the constructed writer.
         *
         * @param enableValidation whether validation should be enabled
         * @return this builder for method chaining.
         */
        public Builder<T> withValidation(boolean enableValidation) {
            builder.withValidation(enableValidation);
            return this;
        }

        /**
         * Set the Parquet format page size used by the constructed writer.
         *
         * @param pageSize an integer size in bytes
         * @return this builder for method chaining.
         */
        public Builder<T> withPageSize(int pageSize) {
            builder.withPageSize(pageSize);
            return this;
        }

        /**
         * Sets the Parquet format row group row count limit used by the constructed
         * writer.
         *
         * @param rowCount limit for the number of rows stored in a row group
         * @return this builder for method chaining
         */
        public Builder<T> withRowGroupRowCountLimit(int rowCount) {
            builder.withRowGroupRowCountLimit(rowCount);
            return this;
        }

        /**
         * Sets the Parquet format page row count limit used by the constructed writer.
         *
         * @param rowCount limit for the number of rows stored in a page
         * @return this builder for method chaining
         */
        public Builder<T> withPageRowCountLimit(int rowCount) {
            builder.withPageRowCountLimit(rowCount);
            return this;
        }

        /**
         * Set the Parquet format dictionary page size used by the constructed writer.
         *
         * @param dictionaryPageSize an integer size in bytes
         * @return this builder for method chaining.
         */
        public Builder<T> withDictionaryPageSize(int dictionaryPageSize) {
            builder.withDictionaryPageSize(dictionaryPageSize);
            return this;
        }

        /**
         * Enables dictionary encoding for the constructed writer.
         *
         * @return this builder for method chaining.
         */
        public Builder<T> enableDictionaryEncoding() {
            builder.enableDictionaryEncoding();
            return this;
        }

        /**
         * Enable or disable dictionary encoding for the constructed writer.
         *
         * @param enableDictionary whether dictionary encoding should be enabled
         * @return this builder for method chaining.
         */
        public Builder<T> withDictionaryEncoding(boolean enableDictionary) {
            builder.withDictionaryEncoding(enableDictionary);
            return this;
        }

        /**
         * Enable or disable BYTE_STREAM_SPLIT encoding for FLOAT and DOUBLE columns.
         *
         * @param enableByteStreamSplit whether BYTE_STREAM_SPLIT encoding should be
         *                              enabled
         * @return this builder for method chaining.
         */
        public Builder<T> withByteStreamSplitEncoding(boolean enableByteStreamSplit) {
            builder.withByteStreamSplitEncoding(enableByteStreamSplit);
            return this;
        }

        /**
         * Enable or disable dictionary encoding of the specified column for the
         * constructed writer.
         *
         * @param columnPath       the path of the column (dot-string)
         * @param enableDictionary whether dictionary encoding should be enabled
         * @return this builder for method chaining.
         */
        public Builder<T> withDictionaryEncoding(String columnPath, boolean enableDictionary) {
            builder.withDictionaryEncoding(columnPath, enableDictionary);
            return this;
        }

        /**
         * Set the {@link WriterVersion format version} used by the constructed writer.
         *
         * @param version a {@code WriterVersion}
         * @return this builder for method chaining.
         */
        public Builder<T> withWriterVersion(WriterVersion version) {
            builder.withWriterVersion(version);
            return this;
        }

        /**
         * Enables writing page level checksums for the constructed writer.
         *
         * @return this builder for method chaining.
         */
        public Builder<T> enablePageWriteChecksum() {
            builder.enablePageWriteChecksum();
            return this;
        }

        /**
         * Enables writing page level checksums for the constructed writer.
         *
         * @param enablePageWriteChecksum whether page checksums should be written out
         * @return this builder for method chaining.
         */
        public Builder<T> withPageWriteChecksumEnabled(boolean enablePageWriteChecksum) {
            builder.withPageWriteChecksumEnabled(enablePageWriteChecksum);
            return this;
        }

        /**
         * Set max Bloom filter bytes for related columns.
         *
         * @param maxBloomFilterBytes the max bytes of a Bloom filter bitset for a
         *                            column.
         * @return this builder for method chaining
         */
        public Builder<T> withMaxBloomFilterBytes(int maxBloomFilterBytes) {
            builder.withMaxBloomFilterBytes(maxBloomFilterBytes);
            return this;
        }

        /**
         * Sets the NDV (number of distinct values) for the specified column.
         *
         * @param columnPath the path of the column (dot-string)
         * @param ndv        the NDV of the column
         *
         * @return this builder for method chaining.
         */
        public Builder<T> withBloomFilterNDV(String columnPath, long ndv) {
            builder.withBloomFilterNDV(columnPath, ndv);
            return this;
        }

        public Builder<T> withBloomFilterFPP(String columnPath, double fpp) {
            builder.withBloomFilterFPP(columnPath, fpp);
            return this;
        }

        /**
         * When NDV (number of distinct values) for a specified column is not set,
         * whether to use `AdaptiveBloomFilter` to automatically adjust the BloomFilter
         * size according to `parquet.bloom.filter.max.bytes`
         *
         * @param enabled whether to write bloom filter for the column
         */
        public Builder<T> withAdaptiveBloomFilterEnabled(boolean enabled) {
            builder.withAdaptiveBloomFilterEnabled(enabled);
            return this;
        }

        /**
         * When `AdaptiveBloomFilter` is enabled, set how many bloom filter candidates
         * to use.
         *
         * @param columnPath the path of the column (dot-string)
         * @param number     the number of candidate
         */
        public Builder<T> withBloomFilterCandidateNumber(String columnPath, int number) {
            builder.withBloomFilterCandidateNumber(columnPath, number);
            return this;
        }

        /**
         * Sets the bloom filter enabled/disabled
         *
         * @param enabled whether to write bloom filters
         * @return this builder for method chaining
         */
        public Builder<T> withBloomFilterEnabled(boolean enabled) {
            builder.withBloomFilterEnabled(enabled);
            return this;
        }

        /**
         * Sets the bloom filter enabled/disabled for the specified column. If not set
         * for the column specifically the default enabled/disabled state will take
         * place. See {@link #withBloomFilterEnabled(boolean)}.
         *
         * @param columnPath the path of the column (dot-string)
         * @param enabled    whether to write bloom filter for the column
         * @return this builder for method chaining
         */
        public Builder<T> withBloomFilterEnabled(String columnPath, boolean enabled) {
            builder.withBloomFilterEnabled(columnPath, enabled);
            return this;
        }

        /**
         * Sets the minimum number of rows to write before a page size check is done.
         *
         * @param min writes at least `min` rows before invoking a page size check
         * @return this builder for method chaining
         */
        public Builder<T> withMinRowCountForPageSizeCheck(int min) {
            builder.withMinRowCountForPageSizeCheck(min);
            return this;
        }

        /**
         * Sets the maximum number of rows to write before a page size check is done.
         *
         * @param max makes a page size check after `max` rows have been written
         * @return this builder for method chaining
         */
        public Builder<T> withMaxRowCountForPageSizeCheck(int max) {
            builder.withMaxRowCountForPageSizeCheck(max);
            return this;
        }

        /**
         * Sets the length to be used for truncating binary values in a binary column
         * index.
         *
         * @param length the length to truncate to
         * @return this builder for method chaining
         */
        public Builder<T> withColumnIndexTruncateLength(int length) {
            builder.withColumnIndexTruncateLength(length);
            return this;
        }

        /**
         * Sets the length which the min/max binary values in row groups are truncated
         * to.
         *
         * @param length the length to truncate to
         * @return this builder for method chaining
         */
        public Builder<T> withStatisticsTruncateLength(int length) {
            builder.withStatisticsTruncateLength(length);
            return this;
        }

        /**
         * Sets the statistics enabled/disabled for the specified column. All column
         * statistics are enabled by default.
         *
         * @param columnPath the path of the column (dot-string)
         * @param enabled    whether to write calculate statistics for the column
         * @return this builder for method chaining
         */
        public Builder<T> withStatisticsEnabled(String columnPath, boolean enabled) {
            builder.withStatisticsEnabled(columnPath, enabled);
            return this;
        }

        /**
         * Sets whether statistics are enabled globally. When disabled, statistics will
         * not be collected for any column unless explicitly enabled for specific
         * columns.
         *
         * @param enabled whether to collect statistics globally
         * @return this builder for method chaining
         */
        public Builder<T> withStatisticsEnabled(boolean enabled) {
            builder.withStatisticsEnabled(enabled);
            return this;
        }

        /**
         * Sets the size statistics enabled/disabled for the specified column. All
         * column size statistics are enabled by default.
         *
         * @param columnPath the path of the column (dot-string)
         * @param enabled    whether to collect size statistics for the column
         * @return this builder for method chaining
         */
        public Builder<T> withSizeStatisticsEnabled(String columnPath, boolean enabled) {
            builder.withSizeStatisticsEnabled(columnPath, enabled);
            return this;
        }

        /**
         * Sets whether size statistics are enabled globally. When disabled, size
         * statistics will not be collected for any column unless explicitly enabled for
         * specific columns.
         *
         * @param enabled whether to collect size statistics globally
         * @return this builder for method chaining
         */
        public Builder<T> withSizeStatisticsEnabled(boolean enabled) {
            builder.withSizeStatisticsEnabled(enabled);
            return this;
        }

        /**
         * Sets a property that will be available to the read path. For writers that use
         * a Hadoop configuration, this is the recommended way to add configuration
         * values.
         *
         * @param property a String property name
         * @param value    a String property value
         * @return this builder for method chaining.
         */
        public Builder<T> config(String property, String value) {
            builder.config(property, value);
            parquetConfProvided = true;
            return this;
        }

        /**
         * Adds to writer metadata to include in the generated parquet file.
         *
         * @param extraMetaData to add
         * @return this builder for method chaining.
         */
        public Builder<T> withExtraMetaData(Map<String, String> extraMetaData) {
            builder.withExtraMetaData(extraMetaData);
            return this;
        }

        /**
         * Adds to writer metadata to include in the generated parquet file.
         *
         * @param key   of the metadata to add
         * @param value of the metadata to add
         * @return this builder for method chaining.
         */
        public Builder<T> withExtraMetaData(String key, String value) {
            builder.withExtraMetaData(key, value);
            return this;
        }

        /**
         * Sets the ByteBuffer allocator instance to be used for allocating memory for
         * writing.
         *
         * @param allocator the allocator instance
         * @return this builder for method chaining
         */
        public Builder<T> withAllocator(ByteBufferAllocator allocator) {
            builder.withAllocator(allocator);
            return this;
        }

        /**
         * Sets the type of collections type that will be generated following the
         * <a href=
         * "https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">LogicalTypes
         * definition</a>
         *
         * If not configured, 3-level structure is used
         *
         * @param annotatedLevels an Enum configuring the number of levels
         * @return this builder for method chaining.
         */
        public Builder<T> withLevelStructure(AnnotatedLevels annotatedLevels) {
            builder.withLevelStructure(annotatedLevels);
            return this;
        }

        /**
         * Sets the strategy to use generating parquet message and record field names in
         * the schema
         *
         * @param columnNamingStrategy an Enum configuring the strategy to use
         * @return this builder for method chaining.
         */
        public Builder<T> withColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
            builder.withColumnNamingStrategy(columnNamingStrategy);
            return this;
        }

        /**
         * Sets the time unit resolution writing TIME or TIMESTAMP fields:
         * <ul>
         * <li>milliseconds</li>
         * <li>microseconds</li>
         * <li>nanoseconds</li>
         *
         * @param defaultTimeUnit an Enum configuring the resolution to use
         * @return this builder for method chaining.
         */
        public Builder<T> withDefaultTimeUnit(TimeUnit defaultTimeUnit) {
            builder.withDefaultTimeUnit(defaultTimeUnit);
            return this;
        }

        /**
         * Sets Decimal precision and scale
         *
         * @param precision of the decimal number
         * @param scale     of the decimal number
         * @return this builder for method chaining.
         */
        public Builder<T> withDefaultDecimal(int precision, int scale) {
            builder.withDefaultDecimal(precision, scale);
            return this;
        }

        /**
         * Sets the default scale for BigDecimal fields. This is used to adjust the
         * scale of the BigDecimal values to the desired scale.
         *
         * @param roundingMode to use
         * @return this builder for method chaining.
         */
        public Builder<T> withBigDecimalScaleAdjustment(RoundingMode roundingMode) {
            builder.withBigDecimalScaleAdjustment(roundingMode);
            return this;
        }

        /**
         * Configures the factory of the write data model to use, instead of default
         * record convention. The factory receives all configuration to decide how to
         * build the WriteRecordModelType.
         *
         * @param writeModelFactory creates WriteRecordModelType given configuration
         *                          specific to Carpet and Parquet
         * @return this builder for method chaining.
         */
        public Builder<T> withWriteRecordModel(WriteModelFactory<T> writeModelFactory) {
            builder.withWriteRecordModel(writeModelFactory);
            return this;
        }

        /**
         * Configures write data model to use, instead of default record convention.
         *
         * @param rootWriteRecordModel write record model to use
         * @return this builder for method chaining.
         */
        public Builder<T> withWriteRecordModel(WriteRecordModelType<T> rootWriteRecordModel) {
            builder.withWriteRecordModel(rootWriteRecordModel);
            return this;
        }

        public CarpetWriter<T> build() throws IOException {
            return new CarpetWriter<>(buildWriter());
        }

        private ParquetWriter<T> buildWriter() throws IOException {
            if (!parquetConfProvided && !hadoopConfProvided) {
                builder.withConf(new PlainParquetConfiguration());
            }
            return builder.build();
        }

    }

}
