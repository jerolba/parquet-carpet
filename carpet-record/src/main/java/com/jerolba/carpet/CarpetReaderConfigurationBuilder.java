package com.jerolba.carpet;

import java.io.File;
import java.io.IOException;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import com.jerolba.carpet.io.FileSystemInputFile;

/**
 * <p>
 * Abstract builder class for configuring a Carpet Parquet Reader. This class
 * provides methods to set internal configuration options for reading Parquet
 * files, and configure Carpet mapping options.
 * </p>
 * <p>
 * Wraps CarpetParquetReader.Builder, internal builder from Parquet. This class
 * is intended to be extended by specific reader configuration builders that
 * define additional configuration options or behaviors.
 * </p>
 *
 * @param <T>    the type of records being read
 * @param <SELF> the type of the subclass extending this builder
 */
public abstract class CarpetReaderConfigurationBuilder<T, SELF extends CarpetReaderConfigurationBuilder<T, SELF>> {

    private final CarpetParquetReader.Builder<T> builder;

    /**
     *
     * Creates a new {@code Builder} instance from the specified InputFile and
     * record class.
     *
     * @param inputFile   the input file from which the records will be read
     * @param recordClass the class of the records being read
     */
    public CarpetReaderConfigurationBuilder(InputFile file, Class<T> recordClass) {
        this.builder = CarpetParquetReader.builder(file, recordClass);
    }

    /**
     *
     * Creates a new {@code Builder} instance from the specified File and record
     * class.
     *
     * @param file        the File containing the Parquet data
     * @param recordClass the class of the records being read
     */
    public CarpetReaderConfigurationBuilder(File file, Class<T> recordClass) {
        this(new FileSystemInputFile(file), recordClass);
    }

    /**
     * @return this as the correct subclass of
     *         CarpetReaderConfigurationBuilder.Builder.
     */
    protected abstract SELF self();

    /**
     * Returns the InputFile associated with this builder.
     *
     * @return the InputFile
     */
    public InputFile getInputFile() {
        return this.builder.getInputFile();
    }

    /**
     * Returns the configured Class of the records being read.
     *
     * @return Class of the records being read
     */
    public Class<T> getRecordClass() {
        return this.builder.getRecordClass();
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
    public SELF failOnMissingColumn(boolean failOnMissingColumn) {
        this.builder.failOnMissingColumn(failOnMissingColumn);
        return self();
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
    public SELF failOnNullForPrimitives(boolean failOnNullForPrimitives) {
        this.builder.failOnNullForPrimitives(failOnNullForPrimitives);
        return self();
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
    public SELF failNarrowingPrimitiveConversion(boolean failNarrowingPrimitiveConversion) {
        this.builder.failNarrowingPrimitiveConversion(failNarrowingPrimitiveConversion);
        return self();
    }

    public SELF fieldMatchingStrategy(FieldMatchingStrategy fieldMatchingStrategy) {
        this.builder.fieldMatchingStrategy(fieldMatchingStrategy);
        return self();
    }

    public SELF withConf(ParquetConfiguration conf) {
        this.builder.withConf(conf);
        return self();
    }

    public SELF withFilter(Filter filter) {
        this.builder.withFilter(filter);
        return self();
    }

    public SELF withAllocator(ByteBufferAllocator allocator) {
        this.builder.withAllocator(allocator);
        return self();
    }

    public SELF useSignedStringMinMax(boolean useSignedStringMinMax) {
        this.builder.useSignedStringMinMax(useSignedStringMinMax);
        return self();
    }

    public SELF useSignedStringMinMax() {
        this.builder.useSignedStringMinMax();
        return self();
    }

    public SELF useStatsFilter(boolean useStatsFilter) {
        this.builder.useStatsFilter(useStatsFilter);
        return self();
    }

    public SELF useStatsFilter() {
        this.builder.useStatsFilter();
        return self();
    }

    public SELF useDictionaryFilter(boolean useDictionaryFilter) {
        this.builder.useDictionaryFilter(useDictionaryFilter);
        return self();
    }

    public SELF useDictionaryFilter() {
        this.builder.useDictionaryFilter();
        return self();
    }

    public SELF useRecordFilter(boolean useRecordFilter) {
        this.builder.useRecordFilter(useRecordFilter);
        return self();
    }

    public SELF useRecordFilter() {
        this.builder.useRecordFilter();
        return self();
    }

    public SELF useColumnIndexFilter(boolean useColumnIndexFilter) {
        this.builder.useColumnIndexFilter(useColumnIndexFilter);
        return self();
    }

    public SELF useColumnIndexFilter() {
        this.builder.useColumnIndexFilter();
        return self();
    }

    public SELF usePageChecksumVerification(boolean usePageChecksumVerification) {
        this.builder.usePageChecksumVerification(usePageChecksumVerification);
        return self();
    }

    public SELF useBloomFilter(boolean useBloomFilter) {
        this.builder.useBloomFilter(useBloomFilter);
        return self();
    }

    public SELF useBloomFilter() {
        this.builder.useBloomFilter();
        return self();
    }

    public SELF usePageChecksumVerification() {
        this.builder.usePageChecksumVerification();
        return self();
    }

    public SELF withFileRange(long start, long end) {
        this.builder.withFileRange(start, end);
        return self();
    }

    public SELF withCodecFactory(CompressionCodecFactory codecFactory) {
        this.builder.withCodecFactory(codecFactory);
        return self();
    }

    public SELF withDecryption(FileDecryptionProperties fileDecryptionProperties) {
        this.builder.withDecryption(fileDecryptionProperties);
        return self();
    }

    public SELF set(String key, String value) {
        this.builder.set(key, value);
        return self();
    }

    public ParquetReader<T> buildParquetReader() throws IOException {
        return this.builder.build();
    }

}