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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.bson.BsonBinaryWriter;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetParquetWriter;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.ColumnNamingStrategy;
import com.jerolba.carpet.PartitionedCarpetWriter;
import com.jerolba.carpet.PartitionedCarpetWriterBuilder;
import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.annotation.ParquetBson;
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
     * Records with BSON fields are supported. BSON fields are written as Binary
     * columns
     *
     * @throws IOException
     */
    @Test
    void writeBsonField() throws IOException {
        record RecordWithBson(String code, String name, @ParquetBson Binary attributes) {
        }

        var outputFile = new FileSystemOutputFile(temporalFile("BsonField"));
        try (var writer = new CarpetWriter<>(outputFile, RecordWithBson.class)) {
            var spain = new RecordWithBson("ES", "Spain",
                    Binary.fromConstantByteArray(createBsonBinary("population", 48797875)));
            writer.write(spain);
        }
    }

    /**
     * Utility method that creates a BSON binary with the specified id and value
     * using low level BSON API.
     *
     * @param id    The String id to include
     * @param value The double value to include
     * @return byte array containing the BSON binary
     */
    public static byte[] createBsonBinary(String id, double value) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
            writer.writeStartDocument();
            writer.writeName("id");
            writer.writeString(id);
            writer.writeName("value");
            writer.writeDouble(value);
            writer.writeEndDocument();
        }
        byte[] result = buffer.toByteArray();
        buffer.close();
        return result;
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

    /**
     * Write data partitioned by year and category using Hive-style partitioning
     * Creates directory structure: /data/orders/year=2024/category=electronics/data.parquet
     *
     * @throws IOException
     */
    @Test
    void writePartitionedData() throws IOException {
        record Order(long id, String customerId, LocalDateTime orderDate, String category, double amount) {
        }

        // Create test data
        List<Order> orders = List.of(
            new Order(1, "customer1", LocalDateTime.of(2024, 1, 15, 10, 0), "electronics", 299.99),
            new Order(2, "customer2", LocalDateTime.of(2024, 1, 16, 11, 0), "books", 45.50),
            new Order(3, "customer3", LocalDateTime.of(2024, 2, 15, 12, 0), "electronics", 599.99),
            new Order(4, "customer4", LocalDateTime.of(2023, 12, 20, 14, 0), "clothing", 89.99)
        );

        // Configure partitioned writer
        PartitionedCarpetWriter<Order> writer = new PartitionedCarpetWriterBuilder<Order>(Order.class)
            .withBasePath(temporalDirectory("partitioned_orders").getAbsolutePath())
            .partitionBy("year", order -> String.valueOf(order.orderDate().getYear()))
            .partitionBy("category", Order::category)
            .withMaxPathLength(2048)
            .build();

        try {
            // Write all orders - they will be automatically partitioned
            writer.write(orders);
            
            // Verify partitions were created
            assertTrue(writer.getCreatedPartitions().size() > 0);
            
        } finally {
            writer.close();
        }
    }

    /**
     * Write data with complex partitioning logic including business rules
     *
     * @throws IOException
     */
    @Test
    void writePartitionedDataWithBusinessLogic() throws IOException {
        record Transaction(long id, String accountId, LocalDateTime timestamp, double amount, String region) {
        }

        // Create test data
        List<Transaction> transactions = List.of(
            new Transaction(1, "P123", LocalDateTime.of(2024, 1, 15, 10, 0), 1500.00, "US"),
            new Transaction(2, "B456", LocalDateTime.of(2024, 1, 16, 11, 0), 2500.00, "EU"),
            new Transaction(3, "P789", LocalDateTime.of(2024, 2, 15, 12, 0), 500.00, "US"),
            new Transaction(4, "G101", LocalDateTime.of(2024, 2, 16, 13, 0), 10000.00, "AS")
        );

        // Configure partitioned writer with business logic
        PartitionedCarpetWriter<Transaction> writer = new PartitionedCarpetWriterBuilder<Transaction>(Transaction.class)
            .withBasePath(temporalDirectory("partitioned_transactions").getAbsolutePath())
            .partitionBy("year_month", tx -> 
                tx.timestamp().format(java.time.format.DateTimeFormatter.ofPattern("yyyy/MM")))
            .partitionBy("account_type", tx -> {
                // Business logic: categorize accounts
                if (tx.accountId().startsWith("P")) return "personal";
                if (tx.accountId().startsWith("B")) return "business";
                if (tx.accountId().startsWith("G")) return "government";
                return "other";
            })
            .partitionBy("amount_range", tx -> {
                // Business logic: categorize by amount
                if (tx.amount() < 1000) return "small";
                if (tx.amount() < 5000) return "medium";
                if (tx.amount() < 10000) return "large";
                return "xlarge";
            })
            .build();

        try {
            writer.write(transactions);
            assertTrue(writer.getCreatedPartitions().size() > 0);
        } finally {
            writer.close();
        }
    }

    /**
     * Write data with multiple partition levels and error handling
     *
     * @throws IOException
     */
    @Test
    void writePartitionedDataWithMultipleLevels() throws IOException {
        record Event(String userId, String eventType, LocalDateTime timestamp, String region, String device) {
        }

        // Create test data
        List<Event> events = List.of(
            new Event("user1", "login", LocalDateTime.of(2024, 1, 15, 10, 0), "US", "mobile"),
            new Event("user2", "purchase", LocalDateTime.of(2024, 1, 15, 11, 0), "EU", "desktop"),
            new Event("user3", "logout", LocalDateTime.of(2024, 1, 16, 12, 0), "US", "mobile"),
            new Event("user4", "view", LocalDateTime.of(2024, 1, 16, 13, 0), "AS", "tablet")
        );

        // Configure partitioned writer with multiple levels
        PartitionedCarpetWriter<Event> writer = new PartitionedCarpetWriterBuilder<Event>(Event.class)
            .withBasePath(temporalDirectory("partitioned_events").getAbsolutePath())
            .partitionBy("date", event -> 
                event.timestamp().toLocalDate().toString())
            .partitionBy("region_device", event -> 
                event.region() + "_" + event.device())
            .partitionBy("event_category", event -> {
                // Business logic: categorize events
                return switch (event.eventType().toLowerCase()) {
                    case "login", "logout" -> "authentication";
                    case "purchase", "add_to_cart" -> "commerce";
                    case "view", "search" -> "browsing";
                    default -> "other";
                };
            })
            .build();

        try {
            writer.write(events);
            assertTrue(writer.getCreatedPartitions().size() > 0);
        } finally {
            writer.close();
        }
    }



    private File temporalFile(String name) {
        try {
            return Files.createTempFile(name, ".parquet").toFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private File temporalDirectory(String name) {
        try {
            return Files.createTempDirectory(name).toFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
