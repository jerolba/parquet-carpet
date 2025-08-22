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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionedCarpetWriterTest {

    @TempDir
    Path tempDir;

    private String basePath;

    record TestRecord(long id, String category, LocalDateTime timestamp, String region) {}

    @BeforeEach
    void setUp() {
        basePath = tempDir.toString();
    }

    @Test
    void shouldWritePartitionedData() throws IOException {
        // Given
        TestRecord record1 = new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US");
        TestRecord record2 = new TestRecord(2, "books", LocalDateTime.of(2024, 1, 15, 11, 0), "EU");
        TestRecord record3 = new TestRecord(3, "electronics", LocalDateTime.of(2024, 1, 16, 10, 0), "US");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("year", record -> String.valueOf(record.timestamp().getYear()))
            .partitionBy("category", TestRecord::category)
            .build();

        // When
        writer.write(List.of(record1, record2, record3));
        writer.close();

        // Then
        assertTrue(new File(basePath + "/year=2024/category=electronics/data.parquet").exists());
        assertTrue(new File(basePath + "/year=2024/category=books/data.parquet").exists());
        
        // Verify data can be read back
        List<TestRecord> electronicsRecords = new CarpetReader<>(
            new File(basePath + "/year=2024/category=electronics/data.parquet"), 
            TestRecord.class
        ).toList();
        
        assertEquals(2, electronicsRecords.size());
        assertTrue(electronicsRecords.stream().allMatch(r -> r.category().equals("electronics")));
    }

    @Test
    void shouldFailOnNullPartitionValue() throws IOException {
        // Given
        TestRecord record = new TestRecord(1, null, LocalDateTime.of(2024, 1, 15, 10, 0), "US");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("category", TestRecord::category)
            .build();

        // When & Then
        assertThrows(NullPartitionValueException.class, () -> {
            try {
                writer.write(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void shouldFailOnPathTooLong() throws IOException {
        // Given
        TestRecord record = new TestRecord(1, "very_long_category_name_that_exceeds_the_path_limit", 
            LocalDateTime.of(2024, 1, 15, 10, 0), "US");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .withMaxPathLength(50) // Very short limit for testing
            .partitionBy("category", TestRecord::category)
            .build();

        // When & Then
        assertThrows(PathTooLongException.class, () -> {
            try {
                writer.write(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void shouldFailOnDuplicatePartitionKey() {
        // Given & When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath(basePath)
                .partitionBy("category", TestRecord::category)
                .partitionBy("category", TestRecord::region) // Duplicate name
                .build();
        });
    }

    @Test
    void shouldFailOnNullBasePath() {
        // Given & When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath(null)
                .partitionBy("category", TestRecord::category)
                .build();
        });
    }

    @Test
    void shouldFailOnEmptyBasePath() {
        // Given & When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath("")
                .partitionBy("category", TestRecord::category)
                .build();
        });
    }

    @Test
    void shouldFailOnNullPartitionName() {
        // Given & When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath(basePath)
                .partitionBy(null, TestRecord::category)
                .build();
        });
    }

    @Test
    void shouldFailOnEmptyPartitionName() {
        // Given & When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath(basePath)
                .partitionBy("", TestRecord::category)
                .build();
        });
    }

    @Test
    void shouldFailOnNullExtractor() {
        // Given & When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath(basePath)
                .partitionBy("category", null)
                .build();
        });
    }

    @Test
    void shouldFailOnNoPartitionKeys() {
        // Given & When & Then
        assertThrows(IllegalStateException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath(basePath)
                .build();
        });
    }

    @Test
    void shouldFailOnNoBasePath() {
        // Given & When & Then
        assertThrows(IllegalStateException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .partitionBy("category", TestRecord::category)
                .build();
        });
    }

    @Test
    void shouldWriteStreamOfRecords() throws IOException {
        // Given
        Stream<TestRecord> records = Stream.of(
            new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US"),
            new TestRecord(2, "books", LocalDateTime.of(2024, 1, 15, 11, 0), "EU")
        );

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("category", TestRecord::category)
            .build();

        // When
        writer.write(records);
        writer.close();

        // Then
        assertTrue(new File(basePath + "/category=electronics/data.parquet").exists());
        assertTrue(new File(basePath + "/category=books/data.parquet").exists());
    }

    @Test
    void shouldTrackCreatedPartitions() throws IOException {
        // Given
        TestRecord record1 = new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US");
        TestRecord record2 = new TestRecord(2, "books", LocalDateTime.of(2024, 1, 15, 11, 0), "EU");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("category", TestRecord::category)
            .build();

        // When
        writer.write(record1);
        writer.write(record2);

        // Then
        assertEquals(2, writer.getCreatedPartitions().size());
        assertTrue(writer.getCreatedPartitions().contains(basePath + "/category=electronics"));
        assertTrue(writer.getCreatedPartitions().contains(basePath + "/category=books"));
    }

    @Test
    void shouldTrackFailedPartitions() throws IOException {
        // Given
        TestRecord validRecord = new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US");
        TestRecord invalidRecord = new TestRecord(2, null, LocalDateTime.of(2024, 1, 15, 11, 0), "EU");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("category", TestRecord::category)
            .build();

        // When
        writer.write(validRecord);
        
        try {
            writer.write(invalidRecord);
        } catch (NullPartitionValueException e) {
            // Expected
        }

        // Then
        assertEquals(1, writer.getCreatedPartitions().size());
        assertEquals(1, writer.getFailedPartitions().size());
        assertTrue(writer.getFailedPartitions().contains(basePath + "/category=null"));
    }

    @Test
    void shouldCleanupEmptyPartitions() throws IOException {
        // Given
        TestRecord record = new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("category", TestRecord::category)
            .build();

        // When
        writer.write(record);
        
        // Manually delete the data file to simulate empty partition
        File dataFile = new File(basePath + "/category=electronics/data.parquet");
        dataFile.delete();
        
        writer.cleanupEmptyPartitions();

        // Then
        assertFalse(new File(basePath + "/category=electronics").exists());
    }

    @Test
    void shouldValidatePartitionIntegrity() throws IOException {
        // Given
        TestRecord record = new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("category", TestRecord::category)
            .build();

        // When
        writer.write(record);
        writer.validatePartitionIntegrityWithFlush(); // Should not throw

        // Then
        // If we reach here, validation passed
    }

    @Test
    void shouldFailValidationOnMissingDataFile() throws IOException {
        // Given
        TestRecord record = new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("category", TestRecord::category)
            .build();

        // When
        writer.write(record);
        
        // Manually delete the data file
        File dataFile = new File(basePath + "/category=electronics/data.parquet");
        dataFile.delete();

        // Then
        assertThrows(PartitionIntegrityException.class, () -> {
            writer.validatePartitionIntegrity();
        });
    }

    @Test
    void shouldRecoverFromFailures() throws IOException {
        // Given
        TestRecord record = new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("category", TestRecord::category)
            .build();

        // When
        writer.write(record);
        
        // Simulate a failure by adding to failed partitions
        writer.getFailedPartitions().add(basePath + "/category=test");
        
        writer.recoverFromFailures();

        // Then
        assertTrue(writer.getFailedPartitions().isEmpty());
    }

    @Test
    void shouldHandleMultiplePartitionLevels() throws IOException {
        // Given
        TestRecord record = new TestRecord(1, "electronics", LocalDateTime.of(2024, 1, 15, 10, 0), "US");

        PartitionedCarpetWriter<TestRecord> writer = new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
            .withBasePath(basePath)
            .partitionBy("year", r -> String.valueOf(r.timestamp().getYear()))
            .partitionBy("month", r -> String.format("%02d", r.timestamp().getMonthValue()))
            .partitionBy("category", TestRecord::category)
            .partitionBy("region", TestRecord::region)
            .build();

        // When
        writer.write(record);
        writer.close();

        // Then
        String expectedPath = basePath + "/year=2024/month=01/category=electronics/region=US/data.parquet";
        assertTrue(new File(expectedPath).exists());
    }

    @Test
    void shouldHandleInvalidMaxPathLength() {
        // Given & When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath(basePath)
                .withMaxPathLength(0) // Invalid
                .partitionBy("category", TestRecord::category)
                .build();
        });
    }

    @Test
    void shouldHandleNegativeMaxPathLength() {
        // Given & When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            new PartitionedCarpetWriterBuilder<TestRecord>(TestRecord.class)
                .withBasePath(basePath)
                .withMaxPathLength(-1) // Invalid
                .partitionBy("category", TestRecord::category)
                .build();
        });
    }
}
