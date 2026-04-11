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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import com.jerolba.carpet.io.OutputStreamOutputFile;

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
        this.writer = new Builder<>(recordClass).withFile(outputFile).buildParquetWriter();
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
        this(new OutputStreamOutputFile(outputStream), recordClass);
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

    public static class Builder<T> extends CarpetWriterConfigurationBuilder<T, Builder<T>> {

        /**
         * Creates a new {@code Builder} instance from the specified record class.
         *
         * The OutputFile must be set later.
         *
         * @param recordClass the class of the records being written
         */
        public Builder(Class<T> recordClass) {
            super(recordClass);
        }

        /**
         * Creates a new {@code Builder} instance from the specified OutputFile and
         * record class.
         *
         * @param outputFile  the output file to which the records will be written
         * @param recordClass the class of the records being written
         */
        public Builder(OutputFile outputFile, Class<T> recordClass) {
            super(recordClass);
            withFile(outputFile);
        }

        /**
         * Creates a new {@code Builder} instance from the specified OutputStream and
         * record class.
         *
         * @param outputStream the OutputStream to write to
         * @param recordClass  the class of the records being written
         */
        public Builder(OutputStream outputStream, Class<T> recordClass) {
            super(recordClass);
            withFile(new OutputStreamOutputFile(outputStream));
        }

        /**
         * Set the {@link OutputFile} used by the constructed writer.
         *
         * @param outputFile a {@code OutputFile}
         * @return this builder for method chaining.
         */
        @Override
        public Builder<T> withFile(OutputFile outputFile) {
            super.withFile(outputFile);
            return this;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        public CarpetWriter<T> build() throws IOException {
            return new CarpetWriter<>(buildParquetWriter());
        }

    }

}
