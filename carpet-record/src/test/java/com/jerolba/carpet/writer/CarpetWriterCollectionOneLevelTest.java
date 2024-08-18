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
package com.jerolba.carpet.writer;

import static com.jerolba.carpet.AnnotatedLevels.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.RecordTypeConversionException;

//TODO: how can we verify that is correct with out using carpet reader?
class CarpetWriterCollectionOneLevelTest {

    @Test
    void simpleTypeCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Integer> ids, List<Double> amount) {
        }

        var rec = new SimpleTypeCollection("foo", List.of(1, 2, 3), List.of(1.2, 3.2));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        String expected = """
                message SimpleTypeCollection {
                  optional binary name (STRING);
                  repeated int32 ids;
                  repeated double amount;
                }
                """;
        assertEquals(expected, writerTest.getSchema().toString());

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void emptyCollectionIsTransformedToNull() throws IOException {

        record EmptyCollection(String name, List<Integer> ids) {
        }

        var rec = new EmptyCollection("foo", List.of());
        var writerTest = new ParquetWriterTest<>(EmptyCollection.class).withLevel(ONE);
        writerTest.write(rec);

        String expected = """
                message EmptyCollection {
                  optional binary name (STRING);
                  repeated int32 ids;
                }
                """;
        assertEquals(expected, writerTest.getSchema().toString());

        var carpetReader = writerTest.getCarpetReader();
        EmptyCollection expectedNullList = new EmptyCollection("foo", null);
        assertEquals(expectedNullList, carpetReader.read());
    }

    @Test
    void consecutiveNestedCollectionsAreNotSupported() {

        record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
        }

        var rec = new ConsecutiveNestedCollection("foo", List.of(List.of(1, 2, 3)));
        assertThrows(RecordTypeConversionException.class, () -> {
            var writerTest = new ParquetWriterTest<>(ConsecutiveNestedCollection.class).withLevel(ONE);
            writerTest.write(rec);
        });
    }

    @Test
    void simpleCompositeCollection() throws IOException {

        record ChildRecord(String id, boolean active) {
        }

        record SimpleCompositeCollection(String name, List<ChildRecord> ids) {
        }

        var rec = new SimpleCompositeCollection("foo",
                List.of(new ChildRecord("Madrid", true), new ChildRecord("Sevilla", false)));
        var writerTest = new ParquetWriterTest<>(SimpleCompositeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        String expected = """
                message SimpleCompositeCollection {
                  optional binary name (STRING);
                  repeated group ids {
                    optional binary id (STRING);
                    required boolean active;
                  }
                }
                """;
        assertEquals(expected, writerTest.getSchema().toString());

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void nonConsecutiveNestedCollections() throws IOException {

        record ChildCollection(String name, List<String> alias) {
        }
        record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
        }

        var rec = new NonConsecutiveNestedCollection("foo",
                List.of(new ChildCollection("Apple", List.of("MacOs", "OS X"))));
        var writerTest = new ParquetWriterTest<>(NonConsecutiveNestedCollection.class).withLevel(ONE);
        writerTest.write(rec);

        String expected = """
                message NonConsecutiveNestedCollection {
                  optional binary id (STRING);
                  repeated group values {
                    optional binary name (STRING);
                    repeated binary alias (STRING);
                  }
                }
                """;
        assertEquals(expected, writerTest.getSchema().toString());

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void nestedMapInCollection() throws IOException {

        record MapInCollection(String name, List<Map<String, Integer>> ids) {
        }

        var rec = new MapInCollection("foo",
                List.of(Map.of("1", 1, "2", 2, "3", 3), Map.of("1", 10, "2", 20, "3", 30)));
        var writerTest = new ParquetWriterTest<>(MapInCollection.class).withLevel(ONE);
        writerTest.write(rec);

        String expected = """
                message MapInCollection {
                  optional binary name (STRING);
                  repeated group ids (MAP) {
                    repeated group key_value {
                      required binary key (STRING);
                      optional int32 value;
                    }
                  }
                }
                """;
        assertEquals(expected, writerTest.getSchema().toString());

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

}
