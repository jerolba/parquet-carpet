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
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

public class CarpetWriter<T> implements Closeable {

    private final ParquetWriter<T> writer;

    public CarpetWriter(OutputFile path, Class<T> recordClass) throws IOException {
        this.writer = new Builder<>(path, recordClass)
                .withWriteMode(Mode.OVERWRITE)
                .withValidation(true)
                .buildWriter();
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
        for (var v : collection) {
            writer.write(v);
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

        public Builder(OutputFile path, Class<T> recordClass) {
            builder = CarpetParquetWriter.builder(path, recordClass)
                    .withWriteMode(Mode.OVERWRITE)
                    .withValidation(true);
        }

        /**
         * Set the {@link Configuration} used by the constructed writer.
         *
         * @param conf a {@code Configuration}
         * @return this builder for method chaining.
         */
        public Builder<T> withConf(Configuration conf) {
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
         * Set a property that will be available to the read path. For writers that use
         * a Hadoop configuration, this is the recommended way to add configuration
         * values.
         *
         * @param property a String property name
         * @param value    a String property value
         * @return this builder for method chaining.
         */
        public Builder<T> config(String property, String value) {
            builder.config(property, value);
            return this;
        }

        public CarpetWriter<T> build() throws IOException {
            return new CarpetWriter<>(builder.build());
        }

        private ParquetWriter<T> buildWriter() throws IOException {
            return builder.build();
        }

    }
}
