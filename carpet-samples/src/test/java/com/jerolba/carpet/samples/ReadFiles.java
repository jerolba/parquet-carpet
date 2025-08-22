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

import static com.jerolba.carpet.samples.WriteFiles.createBsonBinary;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.bson.BsonBinaryReader;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetMissingColumnException;
import com.jerolba.carpet.CarpetParquetReader;
import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.FieldMatchingStrategy;
import com.jerolba.carpet.PartitionedCarpetWriter;
import com.jerolba.carpet.PartitionedCarpetWriterBuilder;
import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.io.FileSystemInputFile;
import com.jerolba.carpet.io.FileSystemOutputFile;

class ReadFiles {

    record Employee(long id, String name, String role, double salary) {
    }

    File givenEmployeesFile() {
        File file = temporalFile("SimpleRecord");
        var outputFile = new FileSystemOutputFile(file);
        try (CarpetWriter<Employee> writer = new CarpetWriter<>(outputFile, Employee.class)) {
            writer.write(new Employee(1, "John", "CEO", 120000));
            writer.write(new Employee(2, "Ana", "CTO", 90000));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }

    /**
     * Read the whole file directly to a List
     *
     * @throws IOException
     */
    @Test
    void toList() throws IOException {
        File file = givenEmployeesFile();
        List<Employee> employees = new CarpetReader<>(file, Employee.class).toList();
        assertEquals(2, employees.size());
    }

    /**
     * Stream the content of the file, applying all available utilities from Java
     * Streams
     *
     */

    @Test
    void streaming() {
        File file = givenEmployeesFile();
        List<Employee> employees = new CarpetReader<>(file, Employee.class).stream()
                .filter(e -> e.salary() > 100000)
                .toList();
        assertEquals(1, employees.size());
    }

    /**
     * Iterate file content using a Java for loop
     */
    @Test
    void forLoop() {
        File file = givenEmployeesFile();
        CarpetReader<Employee> reader = new CarpetReader<>(file, Employee.class);
        for (Employee employee : reader) {
            assertTrue(employee.salary() > 0);
        }
    }

    /**
     * Iterate file content using a Java Iterator
     */
    @Test
    void iterator() {
        File file = givenEmployeesFile();
        CarpetReader<Employee> reader = new CarpetReader<>(file, Employee.class);
        Iterator<Employee> iterator = reader.iterator();
        while (iterator.hasNext()) {
            Employee employee = iterator.next();
            assertTrue(employee.salary() > 0);
        }
    }

    /**
     * Deserialize the record into a Java Map
     */
    @Test
    void toMap() {
        File file = givenEmployeesFile();
        CarpetReader<Map> reader = new CarpetReader<>(file, Map.class);
        Iterator<Map> iterator = reader.iterator();
        assertEquals(Map.of("id", 1L, "name", "John", "role", "CEO", "salary", 120000.0), iterator.next());
    }

    record Attribute(String name, double value, String unit) {
    }

    record Country(String code, String name, String phonePrefix, List<Attribute> attributes) {
    }

    private final Country spain = new Country("ES", "Spain", "+34", List.of(
            new Attribute("population", 48797875, "person"),
            new Attribute("area", 505990, "km2"),
            new Attribute("GPD", 251600000000.0, "us dollars")));

    /**
     * Read complex data structures into Java Records
     *
     * @throws IOException
     */
    @Test
    void readStructuredRecord() throws IOException {
        var file = temporalFile("StructuredRecord");
        try (var writer = new CarpetWriter<>(new FileSystemOutputFile(file), Country.class)) {
            writer.write(spain);
        }

        CarpetReader<Country> reader = new CarpetReader<>(file, Country.class);
        assertEquals(spain, reader.iterator().next());
    }

    /**
     * Read BSON data into a Java Record through a Binary field
     *
     * @throws IOException
     */
    @Test
    void readRecordWithBson() throws IOException {
        record RecordWithBson(String code, String name, @ParquetBson Binary attribute) {
        }

        var file = temporalFile("RecordWithBson");
        try (var writer = new CarpetWriter<>(new FileSystemOutputFile(file), RecordWithBson.class)) {
            var record = new RecordWithBson("ES", "Spain",
                    Binary.fromConstantByteArray(createBsonBinary("population", 48797875)));
            writer.write(record);
        }

        CarpetReader<RecordWithBson> reader = new CarpetReader<>(file, RecordWithBson.class);
        RecordWithBson recordWithBson = reader.iterator().next();
        assertEquals(new AttributeBson("population", 48797875), decodeBson(recordWithBson.attribute.toByteBuffer()));
    }

    private record AttributeBson(String name, double value) {
    }

    /**
     * Decode BSON data into a Java Record using lower level API
     *
     * @param Binary encoded BSON data
     * @return Decoded AttributeBson
     */
    private AttributeBson decodeBson(ByteBuffer byteBuffer) {
        try (BsonBinaryReader bsonReader = new BsonBinaryReader(byteBuffer)) {
            bsonReader.readStartDocument();
            bsonReader.readName("id");
            String id = bsonReader.readString();
            bsonReader.readName("value");
            double value = bsonReader.readDouble();
            bsonReader.readEndDocument();
            return new AttributeBson(id, value);
        }
    }

    /**
     * File content can be projected to avoid reading columns
     *
     * @throws IOException
     */
    @Test
    void schemaCanBeProjected() throws IOException {
        var file = temporalFile("StructuredRecord");
        try (var writer = new CarpetWriter<>(new FileSystemOutputFile(file), Country.class)) {
            writer.write(spain);
        }

        record AttributeProjected(String name, double value) {
        }
        record CountryProjected(String name, List<AttributeProjected> attributes) {
        }

        var spainProjected = new CountryProjected("Spain", List.of(
                new AttributeProjected("population", 48797875),
                new AttributeProjected("area", 505990),
                new AttributeProjected("GPD", 251600000000.0)));

        CarpetReader<CountryProjected> reader = new CarpetReader<>(file, CountryProjected.class);
        assertEquals(spainProjected, reader.iterator().next());
    }

    /**
     * If unmapped fields exists, fails reading
     *
     * @throws IOException
     */
    @Test
    void unmappedFields() throws IOException {
        File file = givenEmployeesFile();

        record ReadEmployee(long id, String name, String role, double salary, String missingColumn) {
        }

        assertThrows(CarpetMissingColumnException.class, () -> {
            List<ReadEmployee> employees = new CarpetReader<>(file, ReadEmployee.class).toList();
            assertEquals(2, employees.size());
        });
    }

    /**
     * Unmapped fields can be supported if configured
     *
     * @throws IOException
     */
    @Test
    void supportUnmappedFields() throws IOException {
        File file = givenEmployeesFile();

        record ReadEmployee(long id, String name, String role, double salary, long missingColumn) {
        }

        List<ReadEmployee> employees = new CarpetReader<>(file, ReadEmployee.class)
                .withFailOnMissingColumn(false)
                .toList();
        assertEquals(2, employees.size());
        // Values are initialized to 0
        for (var emp : employees) {
            assertEquals(0, emp.missingColumn());
        }
    }

    /**
     * Columns with SNAKE_CASE names can be mapped to Java camel case fields names
     *
     * @throws IOException
     */
    @Test
    void readColumnNamesInSnakeCase() throws IOException {
        record TripWrite(int id, String init_station, String end_station, LocalDateTime departure_time,
                LocalDateTime arraival_time) {
        }

        var file = temporalFile("ColumnNamesInSnakeCase");
        var outputFile = new FileSystemOutputFile(file);
        try (var writer = new CarpetWriter<>(outputFile, TripWrite.class)) {
            var trip = new TripWrite(1, "ST-213", "ST-002", LocalDateTime.of(2024, 8, 31, 10, 58),
                    LocalDateTime.of(2024, 8, 31, 11, 19));
            writer.write(trip);
        }

        record TripRead(int id, String initStation, String endStation, LocalDateTime departureTime,
                LocalDateTime arraivalTime) {
        }
        CarpetReader<TripRead> reader = new CarpetReader<>(file, TripRead.class)
                .withFieldMatchingStrategy(FieldMatchingStrategy.SNAKE_CASE);
        var trip = new TripRead(1, "ST-213", "ST-002", LocalDateTime.of(2024, 8, 31, 10, 58),
                LocalDateTime.of(2024, 8, 31, 11, 19));
        assertEquals(trip, reader.iterator().next());
    }

    /**
     * Columns names can be mapped to Java fields specifying a name
     *
     * @throws IOException
     */
    @Test
    void readColumnNamesWithAlias() throws IOException {
        record TripWrite(int trip_identifier, String initial_station_code, String end_station_code) {
        }

        var file = temporalFile("ColumnNamesWithAlias");
        var outputFile = new FileSystemOutputFile(file);
        try (var writer = new CarpetWriter<>(outputFile, TripWrite.class)) {
            writer.write(new TripWrite(1, "ST-213", "ST-002"));
        }

        record TripRead(@Alias("trip_identifier") int id, @Alias("initial_station_code") String init,
                @Alias("end_station_code") String end) {
        }
        CarpetReader<TripRead> reader = new CarpetReader<>(file, TripRead.class);
        TripRead trip = reader.iterator().next();
        assertEquals(1, trip.id());
        assertEquals("ST-213", trip.init());
        assertEquals("ST-002", trip.end());
    }

    /**
     * BigDecimal values are scaled to selected precision
     *
     * @throws IOException
     */
    @Test
    void configureBigDecimalPrecision() throws IOException {
        record Invoice(long id, String sku, BigDecimal amount) {
        }

        var file = temporalFile("ConfigureBigDecimalPrecission");
        var outputFile = new FileSystemOutputFile(file);
        try (var writer = new CarpetWriter.Builder<>(outputFile, Invoice.class)
                .withDefaultDecimal(10, 3)
                .build()) {
            writer.write(new Invoice(1, "RT-23", new BigDecimal("1202.1")));
        }
        CarpetReader<Invoice> reader = new CarpetReader<>(file, Invoice.class);
        Invoice invoice = reader.iterator().next();
        assertEquals(new BigDecimal("1202.100"), invoice.amount);
        assertNotEquals(new BigDecimal("1202.1000"), invoice.amount);
        assertNotEquals(new BigDecimal("1202.1"), invoice.amount);

    }

    /**
     * Time values seconds resolution is converted
     *
     * @throws IOException
     */

    @Test
    void configureTimeUnitsPrecision() throws IOException {
        record Formula1Lap(String car, int lap, LocalTime finishLapTime) {
        }
        var file = temporalFile("ConfigureTimeUnitsPrecision");
        var outputFile = new FileSystemOutputFile(file);
        try (var writer = new CarpetWriter.Builder<>(outputFile, Formula1Lap.class)
                .withDefaultTimeUnit(TimeUnit.MICROS)
                .build()) {
            writer.write(new Formula1Lap("14", 10, LocalTime.of(15, 16, 23, 318732823)));
        }
        CarpetReader<Formula1Lap> reader = new CarpetReader<>(file, Formula1Lap.class);
        Formula1Lap lap = reader.iterator().next();
        // Nanoseconds information is lost
        assertEquals(new Formula1Lap("14", 10, LocalTime.of(15, 16, 23, 318732000)), lap);
    }

    /**
     * ParquetreaReader creation is available for advanced usage of existing tooling
     *
     * @throws IOException
     */
    @Test
    void createParquetReader() throws IOException {
        var file = givenEmployeesFile();
        FileSystemInputFile inputFile = new FileSystemInputFile(file);
        try (ParquetReader<Employee> reader = CarpetParquetReader.builder(inputFile, Employee.class)
                .useBloomFilter(false)
                .usePageChecksumVerification(true)
                .useStatsFilter(false)
                .build()) {
            Employee emp = null;
            while ((emp = reader.read()) != null) {
                assertTrue(emp.salary() > 10000);
            }
        }
    }

    /**
     * Parquet Filtering feature is available using ParquetreaReader configuration
     *
     * @throws IOException
     */
    @Test
    void filtering() throws IOException {
        var file = givenEmployeesFile();

        FilterPredicate bySalary = and(gt(doubleColumn("salary"), 100000.0), lt(doubleColumn("salary"), 200000.0));
        Gt<Long> byId = gt(longColumn("id"), 0L);
        Filter filter = FilterCompat.get(and(bySalary, byId));

        FileSystemInputFile inputFile = new FileSystemInputFile(file);
        try (ParquetReader<Employee> reader = CarpetParquetReader.builder(inputFile, Employee.class)
                .withFilter(filter)
                .build()) {
            Employee emp = null;
            List<Employee> employees = new ArrayList<>();
            while ((emp = reader.read()) != null) {
                employees.add(emp);
            }
            assertEquals(1, employees.size());
        }
    }

    /**
     * Multiple compression algorithms can be used
     */
    @Nested
    class CompressionAlgorithms {

        record SomeRecord(long id, String name, double value) {
        }

        private File givenCompressedFile(CompressionCodecName codecName) throws IOException {
            File file = temporalFile(codecName.name());
            var outputFile = new FileSystemOutputFile(file);
            try (var writer = new CarpetWriter.Builder<>(outputFile, SomeRecord.class)
                    .withCompressionCodec(codecName)
                    .build()) {
                writer.accept(new SomeRecord(1, "Foo", 123123.31));
                writer.accept(new SomeRecord(2, "Bar", 3493292.23));
            }
            return file;
        }

        /**
         * Data is not compressed
         *
         * @throws IOException
         */
        @Test
        void uncompressed() throws IOException {
            File file = givenCompressedFile(CompressionCodecName.UNCOMPRESSED);
            var data = new CarpetReader<>(file, SomeRecord.class).toList();
            assertTrue(data.contains(new SomeRecord(1, "Foo", 123123.31)));
            assertTrue(data.contains(new SomeRecord(2, "Bar", 3493292.23)));
        }

        /**
         * By default Carpet configures Snappy compression
         *
         * @throws IOException
         */
        @Test
        void snappyByDefault() throws IOException {
            File file = givenCompressedFile(CompressionCodecName.SNAPPY);
            var data = new CarpetReader<>(file, SomeRecord.class).toList();
            assertTrue(data.contains(new SomeRecord(1, "Foo", 123123.31)));
            assertTrue(data.contains(new SomeRecord(2, "Bar", 3493292.23)));
        }

        /**
         * Can read Gzip compression
         *
         * @throws IOException
         */
        @Test
        void gzip() throws IOException {
            File file = givenCompressedFile(CompressionCodecName.GZIP);
            var data = new CarpetReader<>(file, SomeRecord.class).toList();
            assertTrue(data.contains(new SomeRecord(1, "Foo", 123123.31)));
            assertTrue(data.contains(new SomeRecord(2, "Bar", 3493292.23)));
        }

        /**
         * Can read ZSTD compression
         *
         * @throws IOException
         */
        @Test
        void zstd() throws IOException {
            File file = givenCompressedFile(CompressionCodecName.ZSTD);
            var data = new CarpetReader<>(file, SomeRecord.class).toList();
            assertTrue(data.contains(new SomeRecord(1, "Foo", 123123.31)));
            assertTrue(data.contains(new SomeRecord(2, "Bar", 3493292.23)));
        }

        /**
         * Can read LZ4_RAW compression
         *
         * @throws IOException
         */
        @Test
        void lz4Raw() throws IOException {
            File file = givenCompressedFile(CompressionCodecName.LZ4_RAW);
            var data = new CarpetReader<>(file, SomeRecord.class).toList();
            assertTrue(data.contains(new SomeRecord(1, "Foo", 123123.31)));
            assertTrue(data.contains(new SomeRecord(2, "Bar", 3493292.23)));
        }

    }

    /**
     * Read partitioned data using standard CarpetReader for individual partitions
     */
    @Nested
    class PartitionedDataReading {

        record Order(long id, String customerId, LocalDateTime orderDate, String category, double amount) {
        }

        /**
         * Create partitioned data for testing
         */
        private File createPartitionedData() throws IOException {
            // Create test data
            List<Order> orders = List.of(
                new Order(1, "customer1", LocalDateTime.of(2024, 1, 15, 10, 0), "electronics", 299.99),
                new Order(2, "customer2", LocalDateTime.of(2024, 1, 16, 11, 0), "books", 45.50),
                new Order(3, "customer3", LocalDateTime.of(2024, 2, 15, 12, 0), "electronics", 599.99),
                new Order(4, "customer4", LocalDateTime.of(2023, 12, 20, 14, 0), "clothing", 89.99)
            );

            // Create partitioned data
            File baseDir = temporalDirectory("partitioned_orders");
            PartitionedCarpetWriter<Order> writer = new PartitionedCarpetWriterBuilder<Order>(Order.class)
                .withBasePath(baseDir.getAbsolutePath())
                .partitionBy("year", order -> String.valueOf(order.orderDate().getYear()))
                .partitionBy("category", Order::category)
                .build();

            try {
                writer.write(orders);
            } finally {
                writer.close();
            }

            return baseDir;
        }

        /**
         * Read specific partition using standard CarpetReader
         *
         * @throws IOException
         */
        @Test
        void readSpecificPartition() throws IOException {
            File baseDir = createPartitionedData();

            // Read specific partition: 2024 electronics
            File partitionFile = new File(baseDir, "year=2024/category=electronics/data.parquet");
            List<Order> electronics2024 = new CarpetReader<>(partitionFile, Order.class).toList();

            assertEquals(2, electronics2024.size());
            assertTrue(electronics2024.stream().allMatch(order -> 
                order.orderDate().getYear() == 2024 && order.category().equals("electronics")));
        }

        /**
         * Read multiple partitions and combine results
         *
         * @throws IOException
         */
        @Test
        void readMultiplePartitions() throws IOException {
            File baseDir = createPartitionedData();

            // Read all 2024 partitions
            List<Order> all2024Orders = new ArrayList<>();
            
            File year2024Dir = new File(baseDir, "year=2024");
            if (year2024Dir.exists() && year2024Dir.isDirectory()) {
                File[] categoryDirs = year2024Dir.listFiles();
                if (categoryDirs != null) {
                    for (File categoryDir : categoryDirs) {
                        if (categoryDir.isDirectory()) {
                            File dataFile = new File(categoryDir, "data.parquet");
                            if (dataFile.exists()) {
                                List<Order> partitionOrders = new CarpetReader<>(dataFile, Order.class).toList();
                                all2024Orders.addAll(partitionOrders);
                            }
                        }
                    }
                }
            }

            assertEquals(3, all2024Orders.size());
            assertTrue(all2024Orders.stream().allMatch(order -> order.orderDate().getYear() == 2024));
        }

        /**
         * Stream data from multiple partitions
         *
         * @throws IOException
         */
        @Test
        void streamFromMultiplePartitions() throws IOException {
            File baseDir = createPartitionedData();

            // Stream from all partitions
            List<Order> allOrders = new ArrayList<>();
            
            File[] yearDirs = baseDir.listFiles();
            if (yearDirs != null) {
                for (File yearDir : yearDirs) {
                    if (yearDir.isDirectory() && yearDir.getName().startsWith("year=")) {
                        File[] categoryDirs = yearDir.listFiles();
                        if (categoryDirs != null) {
                            for (File categoryDir : categoryDirs) {
                                if (categoryDir.isDirectory()) {
                                    File dataFile = new File(categoryDir, "data.parquet");
                                    if (dataFile.exists()) {
                                        List<Order> partitionOrders = new CarpetReader<>(dataFile, Order.class).toList();
                                        allOrders.addAll(partitionOrders);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            assertEquals(4, allOrders.size());
        }

        /**
         * Filter data by partition values during reading
         *
         * @throws IOException
         */
        @Test
        void filterByPartitionValues() throws IOException {
            File baseDir = createPartitionedData();

            // Read only electronics orders from any year
            List<Order> electronicsOrders = new ArrayList<>();
            
            File[] yearDirs = baseDir.listFiles();
            if (yearDirs != null) {
                for (File yearDir : yearDirs) {
                    if (yearDir.isDirectory() && yearDir.getName().startsWith("year=")) {
                        File electronicsDir = new File(yearDir, "category=electronics");
                        if (electronicsDir.exists() && electronicsDir.isDirectory()) {
                            File dataFile = new File(electronicsDir, "data.parquet");
                            if (dataFile.exists()) {
                                List<Order> partitionOrders = new CarpetReader<>(dataFile, Order.class).toList();
                                electronicsOrders.addAll(partitionOrders);
                            }
                        }
                    }
                }
            }

            assertEquals(2, electronicsOrders.size());
            assertTrue(electronicsOrders.stream().allMatch(order -> order.category().equals("electronics")));
        }

        /**
         * Read partitioned data with business logic filtering
         *
         * @throws IOException
         */
        @Test
        void readWithBusinessLogicFiltering() throws IOException {
            File baseDir = createPartitionedData();

            // Read high-value orders (>$100) from 2024
            List<Order> highValue2024Orders = new ArrayList<>();
            
            File year2024Dir = new File(baseDir, "year=2024");
            if (year2024Dir.exists() && year2024Dir.isDirectory()) {
                File[] categoryDirs = year2024Dir.listFiles();
                if (categoryDirs != null) {
                    for (File categoryDir : categoryDirs) {
                        if (categoryDir.isDirectory()) {
                            File dataFile = new File(categoryDir, "data.parquet");
                            if (dataFile.exists()) {
                                List<Order> partitionOrders = new CarpetReader<>(dataFile, Order.class).toList();
                                // Apply business logic filter
                                List<Order> highValueOrders = partitionOrders.stream()
                                    .filter(order -> order.amount() > 100.0)
                                    .toList();
                                highValue2024Orders.addAll(highValueOrders);
                            }
                        }
                    }
                }
            }

            assertEquals(2, highValue2024Orders.size());
            assertTrue(highValue2024Orders.stream().allMatch(order -> 
                order.orderDate().getYear() == 2024 && order.amount() > 100.0));
        }

        /**
         * Demonstrate reading partitioned data with error handling
         *
         * @throws IOException
         */
        @Test
        void readPartitionedDataWithErrorHandling() throws IOException {
            File baseDir = createPartitionedData();

            // Read all partitions with error handling
            List<Order> allOrders = new ArrayList<>();
            List<String> failedPartitions = new ArrayList<>();
            
            File[] yearDirs = baseDir.listFiles();
            if (yearDirs != null) {
                for (File yearDir : yearDirs) {
                    if (yearDir.isDirectory() && yearDir.getName().startsWith("year=")) {
                        File[] categoryDirs = yearDir.listFiles();
                        if (categoryDirs != null) {
                            for (File categoryDir : categoryDirs) {
                                if (categoryDir.isDirectory()) {
                                    File dataFile = new File(categoryDir, "data.parquet");
                                    if (dataFile.exists()) {
                                        try {
                                            List<Order> partitionOrders = new CarpetReader<>(dataFile, Order.class).toList();
                                            allOrders.addAll(partitionOrders);
                                        } catch (Exception e) {
                                            // Log failed partition
                                            failedPartitions.add(categoryDir.getAbsolutePath());
                                            System.err.println("Failed to read partition: " + categoryDir.getAbsolutePath() + " - " + e.getMessage());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            assertEquals(4, allOrders.size());
            assertEquals(0, failedPartitions.size()); // Should be 0 for valid data
        }

        private File temporalDirectory(String name) {
            try {
                return Files.createTempDirectory(name).toFile();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
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
