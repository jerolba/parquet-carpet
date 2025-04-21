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
package com.jerolba.carpet.samples;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetParquetWriter;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.ColumnNamingStrategy;
import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.io.FileSystemOutputFile;

class WriteFiles {

    /**
     * You can write values one by one, using a collection or a stream
     *
     * @throws IOException
     */
    @Test
    void writeSimpleRecord() throws IOException {
        record Employee(long id, String name, String role, double salary) {
        }

        var outputFile = new FileSystemOutputFile(temporalFile("SimpleRecord"));
        try (CarpetWriter<Employee> writer = new CarpetWriter<>(outputFile, Employee.class)) {

            // row by row
            writer.write(new Employee(1, "John", "CEO", 120000));
            writer.write(new Employee(2, "Ana", "CTO", 90000));

            // Collection
            List<Employee> employees = List.of(
                    new Employee(3, "Peter", "CFO", 95000),
                    new Employee(4, "Rose", "CPO", 90000));
            writer.write(employees);

            // Collection
            Stream<Employee> stream = Stream.of(
                    new Employee(5, "Adam", "VP Eng", 80000),
                    new Employee(6, "Maria", "VP Infra", 79000));
            writer.write(stream);
        }
    }


    @Test
    void writeBinaryField() throws IOException {
        record Employee(long id, String name, Binary bytes) {
        }

        var outputFile = new FileSystemOutputFile(temporalFile("BinaryRecord"));
        try (CarpetWriter<Employee> writer = new CarpetWriter<>(outputFile, Employee.class)) {

            // row by row
            writer.write(new Employee(1, "John", Binary.fromConstantByteArray(new byte[] { 1, 2, 3 })));
            writer.write(new Employee(2, "Ana", Binary.fromConstantByteArray(new byte[] { 4, 5, 6 })));
        }
    }


    /**
     * Records with nested complex data structures are supported
     *
     * @throws IOException
     */
    @Test
    void writeStructuredRecord() throws IOException {
        record Attribute(String name, double value, String unit) {
        }
        record Country(String code, String name, String phonePrefix, List<Attribute> attributes) {
        }

        var outputFile = new FileSystemOutputFile(temporalFile("StructuredRecord"));
        try (var writer = new CarpetWriter<>(outputFile, Country.class)) {
            var spain = new Country("ES", "Spain", "+34", List.of(
                    new Attribute("population", 48797875, "person"),
                    new Attribute("area", 505990, "km2"),
                    new Attribute("GPD", 251600000000.0, "us dollars")));
            writer.write(spain);
        }
    }

    /**
     * Java fields names can be converted to SNAKE_CASE column names
     *
     * @throws IOException
     */
    @Test
    void writeColumnNamesInSnakeCase() throws IOException {
        record Trip(UUID code, String initStation, String endStation, LocalDateTime departureTime,
                LocalDateTime arrivalTime) {
        }

        var outputFile = new FileSystemOutputFile(temporalFile("ColumnNamesInSnakeCase"));
        try (var writer = new CarpetWriter.Builder<>(outputFile, Trip.class)
                .withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE)
                .build()) {
            var trip = new Trip(randomUUID(), "ST-213", "ST-002", LocalDateTime.of(2024, 8, 31, 10, 58),
                    LocalDateTime.of(2024, 8, 31, 11, 19));
            writer.write(trip);
        }
    }

    /**
     * Characters like ' ' and '#' are not supported in Java field names. You can
     * use an alias on a field to completely rename the column name
     *
     * @throws IOException
     */
    @Test
    void writeColumnNamesWithAlias() throws IOException {
        // # and space are not a valid in Java field names
        record Trip(UUID code, @Alias("#init station") String initStation, @Alias("#end station") String endStation) {
        }

        var outputFile = new FileSystemOutputFile(temporalFile("ColumnNamesWithAlias"));
        try (var writer = new CarpetWriter<>(outputFile, Trip.class)) {
            writer.write(new Trip(randomUUID(), "ST-213", "ST-002"));
        }
    }

    /**
     * BigDecimal values are written with a precision and scale, that must be
     * configured if a BigDecimal column is present in the file
     *
     * @throws IOException
     */
    @Test
    void configureBigDecimalPrecision() throws IOException {
        record Invoice(long id, String sku, BigDecimal amount) {
        }

        var outputFile = new FileSystemOutputFile(temporalFile("ConfigureBigDecimalPrecission"));
        try (var writer = new CarpetWriter.Builder<>(outputFile, Invoice.class)
                .withDefaultDecimal(10, 2)
                .build()) {
            writer.write(new Invoice(1, "RT-2348204347", new BigDecimal("1202.1")));
            writer.write(new Invoice(2, "PR-3483678493", new BigDecimal("243324.12")));
        }
    }

    /**
     * Time values are written with millisecond resolution, but can be changed.
     *
     * @throws IOException
     */

    @Test
    void configureTimeUnitsPrecision() throws IOException {
        record Formula1Lap(String car, int lap, LocalTime finishLapTime) {
        }
        var outputFile = new FileSystemOutputFile(temporalFile("ConfigureTimeUnitsPrecision"));
        try (var writer = new CarpetWriter.Builder<>(outputFile, Formula1Lap.class)
                .withDefaultTimeUnit(TimeUnit.NANOS)
                .build()) {
            writer.write(new Formula1Lap("14", 10, LocalTime.of(15, 16, 23, 318732823)));
            writer.write(new Formula1Lap("1", 10, LocalTime.of(15, 16, 25, 234987289)));
        }
    }

    /**
     * Internal Parquet configuration is exposed by CarpetWriter builder
     *
     * @throws IOException
     */
    @Test
    void tuneParquetParameters() throws IOException {
        record SomeRecord(long id, String code, double value) {
        }
        var outputFile = new FileSystemOutputFile(temporalFile("TuneParquetParameters"));
        try (var writer = new CarpetWriter.Builder<>(outputFile, SomeRecord.class)
                .withValidation(true)
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(30000)
                .withRowGroupSize(256 * 1024 * 1024)
                .build()) {
            writer.write(new SomeRecord(1, "AS-3932929", 100231.20));
        }
    }

    /**
     * ParquetWriter creation is available for advanced usage of existing tooling
     *
     * @throws IOException
     */
    @Test
    void createParquetWriter() throws IOException {
        record SomeRecord(long id, String code, double value) {
        }
        var outputFile = new FileSystemOutputFile(temporalFile("CreateParquetWriter"));
        try (ParquetWriter<SomeRecord> writer = CarpetParquetWriter.builder(outputFile, SomeRecord.class)
                .withValidation(true)
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(30000)
                .withRowGroupSize(256L * 1024 * 1024)
                .withBloomFilterEnabled(false)
                .withWriteMode(Mode.OVERWRITE)
                .build()) {
            writer.write(new SomeRecord(1, "AS-3932929", 100231.20));
            // Stream and collection write is not supported
            // writer.write(List.of(new SomeRecord(1, "AS-3932929", 100231.20)));
        }
    }

    /**
     * Multiple compression algorithms can be used
     */
    @Nested
    class CompressionAlgorithms {

        record SomeRecord(long id, String name, double value) {
        }

        /**
         * Data is not compressed
         *
         * @throws IOException
         */
        @Test
        void uncompressed() throws IOException {
            var outputFile = new FileSystemOutputFile(temporalFile("Uncompressed"));
            try (var writer = new CarpetWriter.Builder<>(outputFile, SomeRecord.class)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .build()) {
                processRecords(writer);
            }
        }

        /**
         * By default Carpet configures Snappy compression
         *
         * @throws IOException
         */
        @Test
        void snappyByDefault() throws IOException {
            var outputFile = new FileSystemOutputFile(temporalFile("SnappyByDefault"));
            try (var writer = new CarpetWriter<>(outputFile, SomeRecord.class)) {
                processRecords(writer);
            }
        }

        /**
         * Can configure Gzip compression
         *
         * @throws IOException
         */
        @Test
        void gzip() throws IOException {
            var outputFile = new FileSystemOutputFile(temporalFile("Gzip"));
            try (var writer = new CarpetWriter.Builder<>(outputFile, SomeRecord.class)
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .build()) {
                processRecords(writer);
            }
        }

        /**
         * Can configure ZSTD compression
         *
         * @throws IOException
         */
        @Test
        void zstd() throws IOException {
            var outputFile = new FileSystemOutputFile(temporalFile("Zstd"));
            try (var writer = new CarpetWriter.Builder<>(outputFile, SomeRecord.class)
                    .withCompressionCodec(CompressionCodecName.ZSTD)
                    .build()) {
                processRecords(writer);
            }
        }

        /**
         * Can configure LZ4_RAW compression
         *
         * @throws IOException
         */
        @Test
        void lz4Raw() throws IOException {
            var outputFile = new FileSystemOutputFile(temporalFile("Lz4raw"));
            try (var writer = new CarpetWriter.Builder<>(outputFile, SomeRecord.class)
                    .withCompressionCodec(CompressionCodecName.LZ4_RAW)
                    .build()) {
                processRecords(writer);
            }
        }

        /**
         * LZ4 can be configured, but required code is not included. org.lz4:lz4-java
         * dependency must be added.
         *
         * @throws IOException
         */
        @Test
        void lz4() throws IOException {
            assertThrows(NoClassDefFoundError.class, () -> {
                var outputFile = new FileSystemOutputFile(temporalFile("Lz4"));
                try (var writer = new CarpetWriter.Builder<>(outputFile, SomeRecord.class)
                        .withCompressionCodec(CompressionCodecName.LZ4)
                        .build()) {
                    processRecords(writer);
                }
            });
        }

        /**
         * LZO can be configured, but required code is not included.
         * org.anarres.lzo:lzo-hadoop dependency must be added.
         *
         * @throws IOException
         */
        @Test
        void lzo() throws IOException {
            assertThrows(BadConfigurationException.class, () -> {
                var outputFile = new FileSystemOutputFile(temporalFile("Lzo"));
                try (var writer = new CarpetWriter.Builder<>(outputFile, SomeRecord.class)
                        .withCompressionCodec(CompressionCodecName.LZO)
                        .build()) {
                    processRecords(writer);
                }
            });
        }

        // CarpetWriter implements Consumer<T> interface
        private void processRecords(Consumer<SomeRecord> consumer) {
            consumer.accept(new SomeRecord(1, "Foo", 123123.31));
            consumer.accept(new SomeRecord(2, "Bar", 3493292.23));
        }

    }

    private File temporalFile(String name) {
        try {
            return Files.createTempFile(name, ".parquet").toFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
