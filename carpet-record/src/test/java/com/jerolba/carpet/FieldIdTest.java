/**
 * Copyright 2025 Jerónimo López Bezanilla
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jerolba.carpet.annotation.FieldId;

class FieldIdTest {

    @Test
    void simpleRecordWithFieldIds(@TempDir Path tempDir) throws IOException {
        record SimpleRecord(
            @FieldId(1) String uuid,
            @FieldId(2) int statusCode,
            @FieldId(3) long durationMillis,
            @FieldId(4) String error) {
        }

        var filePath = tempDir.resolve("test.parquet");
        var outputFile = new LocalOutputFile(filePath);
        try (var writer = new CarpetWriter<>(outputFile, SimpleRecord.class)) {
            writer.write(new SimpleRecord("test-uuid", 200, 1000L, null));
        }

        // Read schema from file
        MessageType schema;
        try (var reader = ParquetFileReader.open(new LocalInputFile(filePath))) {
            schema = reader.getFooter().getFileMetaData().getSchema();
        }

        // Verify field IDs are set
        Type uuidField = schema.getType("uuid");
        assertEquals(1, uuidField.getId().intValue());

        Type statusCodeField = schema.getType("statusCode");
        assertEquals(2, statusCodeField.getId().intValue());

        Type durationMillisField = schema.getType("durationMillis");
        assertEquals(3, durationMillisField.getId().intValue());

        Type errorField = schema.getType("error");
        assertEquals(4, errorField.getId().intValue());
    }

    @Test
    void mixedRecordWithAndWithoutFieldIds(@TempDir Path tempDir) throws IOException {
        record MixedRecord(@FieldId(10) String id, String name, @FieldId(20) int value) {
        }

        var filePath = tempDir.resolve("test.parquet");
        var outputFile = new LocalOutputFile(filePath);
        try (var writer = new CarpetWriter<>(outputFile, MixedRecord.class)) {
            writer.write(new MixedRecord("test-id", "test-name", 42));
        }

        // Read schema from file
        MessageType schema;
        try (var reader = ParquetFileReader.open(new LocalInputFile(filePath))) {
            schema = reader.getFooter().getFileMetaData().getSchema();
        }

        // Verify field IDs are set where specified
        Type idField = schema.getType("id");
        assertEquals(10, idField.getId().intValue());

        Type nameField = schema.getType("name");
        assertNull(nameField.getId());

        Type valueField = schema.getType("value");
        assertEquals(20, valueField.getId().intValue());
    }

    @Test
    void nestedRecordWithFieldIds(@TempDir Path tempDir) throws IOException {
        record ChildRecord(@FieldId(100) String key, @FieldId(101) int value) {}
        record ParentRecord(@FieldId(1) long id, @FieldId(2) String name, @FieldId(3) ChildRecord child) {}

        var filePath = tempDir.resolve("test.parquet");
        var outputFile = new LocalOutputFile(filePath);
        try (var writer = new CarpetWriter<>(outputFile, ParentRecord.class)) {
            writer.write(new ParentRecord(1L, "parent", new ChildRecord("key1", 100)));
        }

        // Read schema from file
        MessageType schema;
        try (var reader = ParquetFileReader.open(new LocalInputFile(filePath))) {
            schema = reader.getFooter().getFileMetaData().getSchema();
        }

        // Verify parent field IDs
        Type idField = schema.getType("id");
        assertEquals(1, idField.getId().intValue());

        Type nameField = schema.getType("name");
        assertEquals(2, nameField.getId().intValue());

        Type childField = schema.getType("child");
        assertEquals(3, childField.getId().intValue());

        // Verify child field IDs
        Type childKeyField = childField.asGroupType().getType("key");
        assertEquals(100, childKeyField.getId().intValue());

        Type childValueField = childField.asGroupType().getType("value");
        assertEquals(101, childValueField.getId().intValue());
    }

    @Test
    void writeWithFieldIds(@TempDir Path tempDir) throws IOException {
        record SomeRecord(
            @FieldId(1) String uuid,
            @FieldId(2) int statusCode,
            @FieldId(3) long durationMillis,
            @FieldId(4) String error) {
        }

        var records = List.of(
            new SomeRecord("uuid-1", 200, 1000L, null),
            new SomeRecord("uuid-2", 404, 2000L, "Not found"),
            new SomeRecord("uuid-3", 500, 3000L, "Internal error"));

        // Write to a file
        var filePath = tempDir.resolve("test.parquet");
        var outputFile = new LocalOutputFile(filePath);
        try (var writer = new CarpetWriter<>(outputFile, SomeRecord.class)) {
            writer.write(records);
        }

        // Verify we can write successfully
        assertTrue(Files.exists(filePath));
        assertTrue(Files.size(filePath) > 0);
    }

    @Test
    void collectionWithFieldIds(@TempDir Path tempDir) throws IOException {
        record CollectionRecord(@FieldId(1) String id, @FieldId(2) List<Integer> values) {}

        var filePath = tempDir.resolve("test.parquet");
        var outputFile = new LocalOutputFile(filePath);
        try (var writer = new CarpetWriter<>(outputFile, CollectionRecord.class)) {
            writer.write(new CollectionRecord("test-id", List.of(1, 2, 3)));
        }

        // Read schema from file
        MessageType schema;
        try (var reader = ParquetFileReader.open(new LocalInputFile(filePath))) {
            schema = reader.getFooter().getFileMetaData().getSchema();
        }

        // Verify field IDs
        Type idField = schema.getType("id");
        assertEquals(1, idField.getId().intValue());

        Type valuesField = schema.getType("values");
        assertEquals(2, valuesField.getId().intValue());
    }

    @Test
    void mapWithFieldIds(@TempDir Path tempDir) throws IOException {
        record MapRecord(@FieldId(1) String id, @FieldId(2) java.util.Map<String, Integer> scores) {}

        var filePath = tempDir.resolve("test.parquet");
        var outputFile = new LocalOutputFile(filePath);
        try (var writer = new CarpetWriter<>(outputFile, MapRecord.class)) {
            writer.write(new MapRecord("test-id", java.util.Map.of("math", 95, "english", 88)));
        }

        // Read schema from file
        MessageType schema;
        try (var reader = ParquetFileReader.open(new LocalInputFile(filePath))) {
            schema = reader.getFooter().getFileMetaData().getSchema();
        }

        // Verify field IDs - only the map field itself should have an ID, not key/value
        Type idField = schema.getType("id");
        assertEquals(1, idField.getId().intValue());

        Type scoresField = schema.getType("scores");
        assertEquals(2, scoresField.getId().intValue());
        
        // Verify that the internal key and value fields do NOT have field IDs
        Type keyValueGroup = scoresField.asGroupType().getType(0);
        Type keyField = keyValueGroup.asGroupType().getType("key");
        Type valueField = keyValueGroup.asGroupType().getType("value");
        assertNull(keyField.getId(), "Map key should not have a field ID");
        assertNull(valueField.getId(), "Map value should not have a field ID");
    }

    @Test
    void complexNestedStructureWithFieldIds(@TempDir Path tempDir) throws IOException {
        record ChildRecord1(
            @FieldId(11) String attribute,
            @FieldId(12) long value
        ) {}

        record ChildRecord2(
            @FieldId(21) String attribute,
            @FieldId(22) long value
        ) {}

        record ChildRecord3(
            @FieldId(31) String attribute,
            @FieldId(32) long value
        ) {}

        record ParentRecord(
            @FieldId(1) String id,
            @FieldId(2) int status,
            @FieldId(3) long durationMillis,
            @FieldId(4) String error,
            @FieldId(5) ChildRecord1 foo,
            @FieldId(6) List<ChildRecord2> bar,
            @FieldId(7) java.util.Map<String, ChildRecord3> baz
        ) {}

        var filePath = tempDir.resolve("test.parquet");
        var outputFile = new LocalOutputFile(filePath);
        try (var writer = new CarpetWriter<>(outputFile, ParentRecord.class)) {
            writer.write(new ParentRecord(
                "id-1",
                200,
                1000L,
                null,
                new ChildRecord1("attr1", 100L),
                List.of(new ChildRecord2("attr2", 200L), new ChildRecord2("attr3", 300L)),
                java.util.Map.of("key1", new ChildRecord3("attr4", 400L))
            ));
        }

        // Read schema from file
        MessageType schema;
        try (var reader = ParquetFileReader.open(new LocalInputFile(filePath))) {
            schema = reader.getFooter().getFileMetaData().getSchema();
        }

        // Verify top-level field IDs
        assertEquals(1, schema.getType("id").getId().intValue());
        assertEquals(2, schema.getType("status").getId().intValue());
        assertEquals(3, schema.getType("durationMillis").getId().intValue());
        assertEquals(4, schema.getType("error").getId().intValue());
        
        // Verify nested record (foo) field IDs
        Type fooField = schema.getType("foo");
        assertEquals(5, fooField.getId().intValue());
        assertEquals(11, fooField.asGroupType().getType("attribute").getId().intValue());
        assertEquals(12, fooField.asGroupType().getType("value").getId().intValue());
        
        // Verify list of records (bar) field IDs
        Type barField = schema.getType("bar");
        assertEquals(6, barField.getId().intValue());
        // Navigate to the element record inside the list
        Type barListGroup = barField.asGroupType().getType("list");
        Type barElement = barListGroup.asGroupType().getType("element");
        assertEquals(21, barElement.asGroupType().getType("attribute").getId().intValue());
        assertEquals(22, barElement.asGroupType().getType("value").getId().intValue());
        
        // Verify map with record values (baz) field IDs
        Type bazField = schema.getType("baz");
        assertEquals(7, bazField.getId().intValue());
        // Navigate to the value record inside the map
        Type bazKeyValue = bazField.asGroupType().getType("key_value");
        Type bazValue = bazKeyValue.asGroupType().getType("value");
        assertEquals(31, bazValue.asGroupType().getType("attribute").getId().intValue());
        assertEquals(32, bazValue.asGroupType().getType("value").getId().intValue());
        
        // Verify that map key doesn't have field ID (it shouldn't)
        Type bazKey = bazKeyValue.asGroupType().getType("key");
        assertNull(bazKey.getId(), "Map key should not have a field ID");
    }
}
