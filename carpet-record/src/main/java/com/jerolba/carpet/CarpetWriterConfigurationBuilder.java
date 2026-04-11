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
package com.jerolba.carpet;

import java.io.IOException;
import java.math.RoundingMode;
import java.util.Map;

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

import com.jerolba.carpet.model.WriteRecordModelType;

/**
 * <p>
 * Abstract builder class for configuring a Carpet Parquet Writer. This class
 * provides methods to set internal configuration options for writing Parquet
 * files, and configure Carpet mapping options.
 * </p>
 * <p>
 * Wraps CarpetParquetWriter.Builder, internal builder from Parquet. This class
 * is intended to be extended by specific writer configuration builders that
 * define additional configuration options or behaviors.
 * </p>
 *
 * @param <T>    the type of records being written
 * @param <SELF> the type of the subclass extending this builder
 */
public abstract class CarpetWriterConfigurationBuilder<T, SELF extends CarpetWriterConfigurationBuilder<T, SELF>> {

    private final CarpetParquetWriter.Builder<T> builder;
    private boolean parquetConfProvided = false;
    private boolean hadoopConfProvided = false;

    /**
     * Creates a new {@code CarpetWriterConfigurationBuilder} instance from the
     * specified record class.
     *
     * The OutputFile must be set later.
     *
     * @param recordClass the class of the records being written
     */
    public CarpetWriterConfigurationBuilder(Class<T> recordClass) {
        this.builder = CarpetParquetWriter.builder(recordClass)
                .withWriteMode(Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY);
    }

    /**
     * @return this as the correct subclass of CarpetWriterConfigurationBuilder
     */
    protected abstract SELF self();

    /**
     * Set the {@link OutputFile} used by the constructed writer.
     *
     * @param outputFile a {@code OutputFile}
     * @return this builder for method chaining.
     */
    protected SELF withFile(OutputFile outputFile) {
        builder.withFile(outputFile);
        return self();
    }

    /**
     * Set the {@link Configuration} used by the constructed writer.
     *
     * @param conf a {@code Configuration}
     * @return this builder for method chaining.
     */
    public SELF withConf(Configuration conf) {
        hadoopConfProvided = true;
        builder.withConf(conf);
        return self();
    }

    /**
     * Set the {@link ParquetConfiguration} used by the constructed writer.
     *
     * @param conf a {@code ParquetConfiguration}
     * @return this builder for method chaining.
     */
    public SELF withConf(ParquetConfiguration conf) {
        parquetConfProvided = true;
        builder.withConf(conf);
        return self();
    }

    /**
     * Set the {@link ParquetFileWriter.Mode write mode} used when creating the
     * backing file for this writer.
     *
     * @param mode a {@code ParquetFileWriter.Mode}
     * @return this builder for method chaining.
     */
    public SELF withWriteMode(ParquetFileWriter.Mode mode) {
        builder.withWriteMode(mode);
        return self();
    }

    /**
     * Set the {@link CompressionCodecName compression codec} used by the
     * constructed writer.
     *
     * @param codecName a {@code CompressionCodecName}
     * @return this builder for method chaining.
     */
    public SELF withCompressionCodec(CompressionCodecName codecName) {
        builder.withCompressionCodec(codecName);
        return self();
    }

    /**
     * Set the {@link CompressionCodecFactory codec factory} used by the constructed
     * writer.
     *
     * @param codecFactory a {@link CompressionCodecFactory}
     * @return this builder for method chaining.
     */
    public SELF withCodecFactory(CompressionCodecFactory codecFactory) {
        builder.withCodecFactory(codecFactory);
        return self();
    }

    /**
     * Set the {@link FileEncryptionProperties file encryption properties} used by
     * the constructed writer.
     *
     * @param encryptionProperties a {@code FileEncryptionProperties}
     * @return this builder for method chaining.
     */
    public SELF withEncryption(FileEncryptionProperties encryptionProperties) {
        builder.withEncryption(encryptionProperties);
        return self();
    }

    /**
     * Set the Parquet format row group size used by the constructed writer.
     *
     * @param rowGroupSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withRowGroupSize(long rowGroupSize) {
        builder.withRowGroupSize(rowGroupSize);
        return self();
    }

    /**
     * Set the maximum amount of padding, in bytes, that will be used to align row
     * groups with blocks in the underlying filesystem. If the underlying filesystem
     * is not a block filesystem like HDFS, this has no effect.
     *
     * @param maxPaddingSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withMaxPaddingSize(int maxPaddingSize) {
        builder.withMaxPaddingSize(maxPaddingSize);
        return self();
    }

    /**
     * Enables validation for the constructed writer.
     *
     * @return this builder for method chaining.
     */
    public SELF enableValidation() {
        builder.enableValidation();
        return self();
    }

    /**
     * Enable or disable validation for the constructed writer.
     *
     * @param enableValidation whether validation should be enabled
     * @return this builder for method chaining.
     */
    public SELF withValidation(boolean enableValidation) {
        builder.withValidation(enableValidation);
        return self();
    }

    /**
     * Set the Parquet format page size used by the constructed writer.
     *
     * @param pageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withPageSize(int pageSize) {
        builder.withPageSize(pageSize);
        return self();
    }

    /**
     * Sets the Parquet format row group row count limit used by the constructed
     * writer.
     *
     * @param rowCount limit for the number of rows stored in a row group
     * @return this builder for method chaining
     */
    public SELF withRowGroupRowCountLimit(int rowCount) {
        builder.withRowGroupRowCountLimit(rowCount);
        return self();
    }

    /**
     * Sets the Parquet format page row count limit used by the constructed writer.
     *
     * @param rowCount limit for the number of rows stored in a page
     * @return this builder for method chaining
     */
    public SELF withPageRowCountLimit(int rowCount) {
        builder.withPageRowCountLimit(rowCount);
        return self();
    }

    /**
     * Set the Parquet format dictionary page size used by the constructed writer.
     *
     * @param dictionaryPageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withDictionaryPageSize(int dictionaryPageSize) {
        builder.withDictionaryPageSize(dictionaryPageSize);
        return self();
    }

    /**
     * Enables dictionary encoding for the constructed writer.
     *
     * @return this builder for method chaining.
     */
    public SELF enableDictionaryEncoding() {
        builder.enableDictionaryEncoding();
        return self();
    }

    /**
     * Enable or disable dictionary encoding for the constructed writer.
     *
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public SELF withDictionaryEncoding(boolean enableDictionary) {
        builder.withDictionaryEncoding(enableDictionary);
        return self();
    }

    /**
     * Enable or disable BYTE_STREAM_SPLIT encoding for FLOAT and DOUBLE columns.
     *
     * @param enableByteStreamSplit whether BYTE_STREAM_SPLIT encoding should be
     *                              enabled
     * @return this builder for method chaining.
     */
    public SELF withByteStreamSplitEncoding(boolean enableByteStreamSplit) {
        builder.withByteStreamSplitEncoding(enableByteStreamSplit);
        return self();
    }

    /**
     * Enable or disable BYTE_STREAM_SPLIT encoding for FLOAT and DOUBLE selected
     * column.
     *
     * @param columnPath            the path of the column (dot-string)
     * @param enableByteStreamSplit whether BYTE_STREAM_SPLIT encoding should be
     *                              enabled
     * @return this builder for method chaining.
     */
    public SELF withByteStreamSplitEncoding(String columnPath, boolean enableByteStreamSplit) {
        builder.withByteStreamSplitEncoding(columnPath, enableByteStreamSplit);
        return self();
    }

    /**
     * Enable or disable dictionary encoding of the specified column for the
     * constructed writer.
     *
     * @param columnPath       the path of the column (dot-string)
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public SELF withDictionaryEncoding(String columnPath, boolean enableDictionary) {
        builder.withDictionaryEncoding(columnPath, enableDictionary);
        return self();
    }

    /**
     * Set the {@link WriterVersion format version} used by the constructed writer.
     *
     * @param version a {@code WriterVersion}
     * @return this builder for method chaining.
     */
    public SELF withWriterVersion(WriterVersion version) {
        builder.withWriterVersion(version);
        return self();
    }

    /**
     * Enables writing page level checksums for the constructed writer.
     *
     * @return this builder for method chaining.
     */
    public SELF enablePageWriteChecksum() {
        builder.enablePageWriteChecksum();
        return self();
    }

    /**
     * Enables writing page level checksums for the constructed writer.
     *
     * @param enablePageWriteChecksum whether page checksums should be written out
     * @return this builder for method chaining.
     */
    public SELF withPageWriteChecksumEnabled(boolean enablePageWriteChecksum) {
        builder.withPageWriteChecksumEnabled(enablePageWriteChecksum);
        return self();
    }

    /**
     * Set max Bloom filter bytes for related columns.
     *
     * @param maxBloomFilterBytes the max bytes of a Bloom filter bitset for a
     *                            column.
     * @return this builder for method chaining
     */
    public SELF withMaxBloomFilterBytes(int maxBloomFilterBytes) {
        builder.withMaxBloomFilterBytes(maxBloomFilterBytes);
        return self();
    }

    /**
     * Sets the NDV (number of distinct values) for the specified column.
     *
     * @param columnPath the path of the column (dot-string)
     * @param ndv        the NDV of the column
     *
     * @return this builder for method chaining.
     */
    public SELF withBloomFilterNDV(String columnPath, long ndv) {
        builder.withBloomFilterNDV(columnPath, ndv);
        return self();
    }

    public SELF withBloomFilterFPP(String columnPath, double fpp) {
        builder.withBloomFilterFPP(columnPath, fpp);
        return self();
    }

    /**
     * When NDV (number of distinct values) for a specified column is not set,
     * whether to use `AdaptiveBloomFilter` to automatically adjust the BloomFilter
     * size according to `parquet.bloom.filter.max.bytes`
     *
     * @param enabled whether to write bloom filter for the column
     * @return this builder for method chaining.
     */
    public SELF withAdaptiveBloomFilterEnabled(boolean enabled) {
        builder.withAdaptiveBloomFilterEnabled(enabled);
        return self();
    }

    /**
     * When `AdaptiveBloomFilter` is enabled, set how many bloom filter candidates
     * to use.
     *
     * @param columnPath the path of the column (dot-string)
     * @param number     the number of candidate
     * @return this builder for method chaining.
     */
    public SELF withBloomFilterCandidateNumber(String columnPath, int number) {
        builder.withBloomFilterCandidateNumber(columnPath, number);
        return self();
    }

    /**
     * Sets the bloom filter enabled/disabled
     *
     * @param enabled whether to write bloom filters
     * @return this builder for method chaining
     */
    public SELF withBloomFilterEnabled(boolean enabled) {
        builder.withBloomFilterEnabled(enabled);
        return self();
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
    public SELF withBloomFilterEnabled(String columnPath, boolean enabled) {
        builder.withBloomFilterEnabled(columnPath, enabled);
        return self();
    }

    /**
     * Sets the minimum number of rows to write before a page size check is done.
     *
     * @param min writes at least `min` rows before invoking a page size check
     * @return this builder for method chaining
     */
    public SELF withMinRowCountForPageSizeCheck(int min) {
        builder.withMinRowCountForPageSizeCheck(min);
        return self();
    }

    /**
     * Sets the maximum number of rows to write before a page size check is done.
     *
     * @param max makes a page size check after `max` rows have been written
     * @return this builder for method chaining
     */
    public SELF withMaxRowCountForPageSizeCheck(int max) {
        builder.withMaxRowCountForPageSizeCheck(max);
        return self();
    }

    /**
     * Sets the length to be used for truncating binary values in a binary column
     * index.
     *
     * @param length the length to truncate to
     * @return this builder for method chaining
     */
    public SELF withColumnIndexTruncateLength(int length) {
        builder.withColumnIndexTruncateLength(length);
        return self();
    }

    /**
     * Sets the length which the min/max binary values in row groups are truncated
     * to.
     *
     * @param length the length to truncate to
     * @return this builder for method chaining
     */
    public SELF withStatisticsTruncateLength(int length) {
        builder.withStatisticsTruncateLength(length);
        return self();
    }

    /**
     * Sets the statistics enabled/disabled for the specified column. All column
     * statistics are enabled by default.
     *
     * @param columnPath the path of the column (dot-string)
     * @param enabled    whether to write calculate statistics for the column
     * @return this builder for method chaining
     */
    public SELF withStatisticsEnabled(String columnPath, boolean enabled) {
        builder.withStatisticsEnabled(columnPath, enabled);
        return self();
    }

    /**
     * Sets whether statistics are enabled globally. When disabled, statistics will
     * not be collected for any column unless explicitly enabled for specific
     * columns.
     *
     * @param enabled whether to collect statistics globally
     * @return this builder for method chaining
     */
    public SELF withStatisticsEnabled(boolean enabled) {
        builder.withStatisticsEnabled(enabled);
        return self();
    }

    /**
     * Sets the size statistics enabled/disabled for the specified column. All
     * column size statistics are enabled by default.
     *
     * @param columnPath the path of the column (dot-string)
     * @param enabled    whether to collect size statistics for the column
     * @return this builder for method chaining
     */
    public SELF withSizeStatisticsEnabled(String columnPath, boolean enabled) {
        builder.withSizeStatisticsEnabled(columnPath, enabled);
        return self();
    }

    /**
     * Sets whether size statistics are enabled globally. When disabled, size
     * statistics will not be collected for any column unless explicitly enabled for
     * specific columns.
     *
     * @param enabled whether to collect size statistics globally
     * @return this builder for method chaining
     */
    public SELF withSizeStatisticsEnabled(boolean enabled) {
        builder.withSizeStatisticsEnabled(enabled);
        return self();
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
    public SELF config(String property, String value) {
        builder.config(property, value);
        parquetConfProvided = true;
        return self();
    }

    /**
     * Adds to writer metadata to include in the generated parquet file.
     *
     * @param extraMetaData to add
     * @return this builder for method chaining.
     */
    public SELF withExtraMetaData(Map<String, String> extraMetaData) {
        builder.withExtraMetaData(extraMetaData);
        return self();
    }

    /**
     * Adds to writer metadata to include in the generated parquet file.
     *
     * @param key   of the metadata to add
     * @param value of the metadata to add
     * @return this builder for method chaining.
     */
    public SELF withExtraMetaData(String key, String value) {
        builder.withExtraMetaData(key, value);
        return self();
    }

    /**
     * Sets the ByteBuffer allocator instance to be used for allocating memory for
     * writing.
     *
     * @param allocator the allocator instance
     * @return this builder for method chaining
     */
    public SELF withAllocator(ByteBufferAllocator allocator) {
        builder.withAllocator(allocator);
        return self();
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
    public SELF withLevelStructure(AnnotatedLevels annotatedLevels) {
        builder.withLevelStructure(annotatedLevels);
        return self();
    }

    /**
     * Sets the strategy to use generating parquet message and record field names in
     * the schema
     *
     * @param columnNamingStrategy an Enum configuring the strategy to use
     * @return this builder for method chaining.
     */
    public SELF withColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
        builder.withColumnNamingStrategy(columnNamingStrategy);
        return self();
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
    public SELF withDefaultTimeUnit(TimeUnit defaultTimeUnit) {
        builder.withDefaultTimeUnit(defaultTimeUnit);
        return self();
    }

    /**
     * Sets Decimal precision and scale
     *
     * @param precision of the decimal number
     * @param scale     of the decimal number
     * @return this builder for method chaining.
     */
    public SELF withDefaultDecimal(int precision, int scale) {
        builder.withDefaultDecimal(precision, scale);
        return self();
    }

    /**
     * Sets the default scale for BigDecimal fields. This is used to adjust the
     * scale of the BigDecimal values to the desired scale.
     *
     * @param roundingMode to use
     * @return this builder for method chaining.
     */
    public SELF withBigDecimalScaleAdjustment(RoundingMode roundingMode) {
        builder.withBigDecimalScaleAdjustment(roundingMode);
        return self();
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
    public SELF withWriteRecordModel(WriteModelFactory<T> writeModelFactory) {
        builder.withWriteRecordModel(writeModelFactory);
        return self();
    }

    /**
     * Configures write data model to use, instead of default record convention.
     *
     * @param rootWriteRecordModel write record model to use
     * @return this builder for method chaining.
     */
    public SELF withWriteRecordModel(WriteRecordModelType<T> rootWriteRecordModel) {
        builder.withWriteRecordModel(rootWriteRecordModel);
        return self();
    }

    /**
     * Builds the ParquetWriter from the current configuration.
     *
     * @return the configured ParquetWriter
     * @throws IOException if an I/O error occurs
     */
    protected ParquetWriter<T> buildParquetWriter() throws IOException {
        if (!parquetConfProvided && !hadoopConfProvided) {
            builder.withConf(new PlainParquetConfiguration());
        }
        return builder.build();
    }

}