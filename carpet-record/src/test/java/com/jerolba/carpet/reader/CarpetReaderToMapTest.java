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
package com.jerolba.carpet.reader;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.AnnotatedLevels;
import com.jerolba.carpet.ParquetWriterTest;

class CarpetReaderToMapTest {

    enum Category {
        one, two, three;
    }

    @Test
    void convertSimpleRecordToMap() throws IOException {

        record InnerRecord(String id, int value) {
        }
        record MainTypeWrite(String name, InnerRecord inner) {
        }

        ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
        var root = new MainTypeWrite("root", new InnerRecord("foo", 100));
        writerTest.write(root);

        record MainTypeRead(String name, Map<String, Object> inner) {
        }

        var reader = writerTest.getCarpetReader(MainTypeRead.class);
        var expected = new MainTypeRead("root", Map.of("id", "foo", "value", 100));
        MainTypeRead actual = reader.read();
        assertEquals(expected, actual);
    }

    @Test
    void convertRecordPrimitiveToMap() throws IOException {

        record InnerRecord(String id, byte asByte, short asShort, int asInt, long asLong, float asFloat,
                double asDouble, boolean asBoolean, Category asEnum, UUID asUuid) {
        }
        record MainTypeWrite(String name, InnerRecord inner) {
        }

        ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
        var root = new MainTypeWrite("root", new InnerRecord("foo", (byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true,
                Category.one, UUID.randomUUID()));
        writerTest.write(root);

        record MainTypeRead(String name, Map<String, Object> inner) {
        }

        var reader = writerTest.getCarpetReader(MainTypeRead.class);
        var expected = new MainTypeRead("root", Map.of(
                "id", "foo",
                "asByte", (byte) 1,
                "asShort", (short) 2,
                "asInt", 3,
                "asLong", 4L,
                "asFloat", 5.0f,
                "asDouble", 6.0,
                "asBoolean", true,
                "asEnum", "one",
                "asUuid", root.inner.asUuid()));
        MainTypeRead actual = reader.read();
        assertEquals(expected, actual);
    }

    @Test
    void convertChainedRecordToMap() throws IOException {

        record ChildRecord(String code, String name) {
        }
        record InnerRecord(String id, ChildRecord value) {
        }
        record MainTypeWrite(String name, InnerRecord inner) {
        }

        ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
        var root = new MainTypeWrite("root", new InnerRecord("foo", new ChildRecord("bar", "Madrid")));
        writerTest.write(root);

        record MainTypeRead(String name, Map<String, Object> inner) {
        }

        var reader = writerTest.getCarpetReader(MainTypeRead.class);
        var expected = new MainTypeRead("root",
                Map.of("id", "foo", "value", Map.of("code", "bar", "name", "Madrid")));
        MainTypeRead actual = reader.read();
        assertEquals(expected, actual);
    }

    @Nested
    class ConvertSingleLevelList {

        @Test
        void convertPrimitiveValues() throws IOException {

            record InnerRecord(String id, List<String> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.ONE);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of("Madrid", "Barcelona")));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of("Madrid", "Barcelona")));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        // TODO: Review behavior: no attribute vs attribute with null value
        void emptyListIsDeserializedAsNoElement() throws IOException {

            record InnerRecord(String id, List<String> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.ONE);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of()));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo"));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertRecordValues() throws IOException {

            record ChildRecord(String code, String name) {
            }
            record InnerRecord(String id, List<ChildRecord> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.ONE);
            var root = new MainTypeWrite("root", new InnerRecord("foo",
                    List.of(new ChildRecord("28", "Madrid"), new ChildRecord("08", "Barcelona"))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(
                    Map.of("code", "28", "name", "Madrid"), Map.of("code", "08", "name", "Barcelona"))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertMapValues() throws IOException {

            record InnerRecord(String id, List<Map<String, String>> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.ONE);
            var root = new MainTypeWrite("root", new InnerRecord("foo",
                    List.of(Map.of("name", "Madrid"), Map.of("name", "Barcelona"))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(
                    Map.of("name", "Madrid"), Map.of("name", "Barcelona"))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertEmptyMapValues() throws IOException {

            record InnerRecord(String id, List<Map<String, String>> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.ONE);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of(Map.of())));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(Map.of())));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

    }

    @Nested
    class ConvertTwoLevelList {

        @Test
        void convertPrimitiveValues() throws IOException {

            record InnerRecord(String id, List<String> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.TWO);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of("Madrid", "Barcelona")));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of("Madrid", "Barcelona")));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        // TODO: Review behavior: no attribute vs attribute with null value
        void emptyListIsDeserializedAsNoElement() throws IOException {

            record InnerRecord(String id, List<String> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.TWO);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of()));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo"));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertRecordValues() throws IOException {

            record ChildRecord(String code, String name) {
            }
            record InnerRecord(String id, List<ChildRecord> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.TWO);
            var root = new MainTypeWrite("root", new InnerRecord("foo",
                    List.of(new ChildRecord("28", "Madrid"), new ChildRecord("08", "Barcelona"))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(
                    Map.of("code", "28", "name", "Madrid"), Map.of("code", "08", "name", "Barcelona"))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertMapValues() throws IOException {

            record InnerRecord(String id, List<Map<String, String>> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.TWO);
            var root = new MainTypeWrite("root", new InnerRecord("foo",
                    List.of(Map.of("name", "Madrid"), Map.of("name", "Barcelona"))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(
                    Map.of("name", "Madrid"), Map.of("name", "Barcelona"))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertEmptyMapValues() throws IOException {

            record InnerRecord(String id, List<Map<String, String>> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.TWO);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of(Map.of())));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(Map.of())));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }
    }

    @Nested
    class ConvertThreeLevelList {

        @Test
        void convertPrimitiveValues() throws IOException {

            record InnerRecord(String id, List<String> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of("Madrid", "Barcelona")));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of("Madrid", "Barcelona")));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void emptyListIsDeserializedAsEmptyList() throws IOException {

            record InnerRecord(String id, List<String> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of()));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", emptyList()));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertRecordValues() throws IOException {

            record ChildRecord(String code, String name) {
            }
            record InnerRecord(String id, List<ChildRecord> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root", new InnerRecord("foo",
                    List.of(new ChildRecord("28", "Madrid"), new ChildRecord("08", "Barcelona"))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(
                    Map.of("code", "28", "name", "Madrid"), Map.of("code", "08", "name", "Barcelona"))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertMapValues() throws IOException {

            record InnerRecord(String id, List<Map<String, String>> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root", new InnerRecord("foo",
                    List.of(Map.of("name", "Madrid"), Map.of("name", "Barcelona"))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(
                    Map.of("name", "Madrid"), Map.of("name", "Barcelona"))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertEmptyMapValues() throws IOException {

            record InnerRecord(String id, List<Map<String, String>> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root", new InnerRecord("foo", List.of(Map.of())));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", List.of(Map.of())));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

    }

    @Nested
    class ConvertInnerMap {

        @Test
        void convertPrimitiveAsMapValues() throws IOException {

            record InnerRecord(String id, Map<String, Integer> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite("root", new InnerRecord("foo", Map.of("Madrid", 120, "Barcelona", 230)));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root",
                    Map.of("id", "foo", "values", Map.of("Madrid", 120, "Barcelona", 230)));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertRecordAsMapValues() throws IOException {

            record ChildRecord(String code, String name) {
            }
            record InnerRecord(String id, Map<String, ChildRecord> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root",
                    new InnerRecord("foo", Map.of(
                            "Madrid", new ChildRecord("Zip", "28"),
                            "Barcelona", new ChildRecord("Zip", "08"))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", Map.of(
                    "Madrid", Map.of("code", "Zip", "name", "28"),
                    "Barcelona", Map.of("code", "Zip", "name", "08"))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertNestedMapAsMapValues() throws IOException {

            record InnerRecord(String id, Map<String, Map<String, String>> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root",
                    new InnerRecord("foo", Map.of(
                            "Madrid", Map.of("Zip", "28"),
                            "Barcelona", Map.of("Zip", "08"))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", Map.of(
                    "Madrid", Map.of("Zip", "28"),
                    "Barcelona", Map.of("Zip", "08"))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertNestedListAsMapValues() throws IOException {

            record InnerRecord(String id, Map<String, List<Integer>> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root",
                    new InnerRecord("foo", Map.of(
                            "Madrid", List.of(1, 2, 3),
                            "Barcelona", List.of(4, 5, 6))));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root", Map.of("id", "foo", "values", Map.of(
                    "Madrid", List.of(1, 2, 3),
                    "Barcelona", List.of(4, 5, 6))));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertKeyRecordAsMapValues() throws IOException {

            record ChildRecord(String code, String name) {
            }
            record InnerRecord(String id, Map<ChildRecord, Integer> values) {
            }
            record MainTypeWrite(String name, InnerRecord inner) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite("root", new InnerRecord("foo", Map.of(
                    new ChildRecord("Madrid", "Population"), 4500000,
                    new ChildRecord("Barcelona", "Population"), 3000000)));
            writerTest.write(root);

            record MainTypeRead(String name, Map<String, Object> inner) {
            }

            var reader = writerTest.getCarpetReader(MainTypeRead.class);
            var expected = new MainTypeRead("root",
                    Map.of("id", "foo", "values", Map.of(
                            Map.of("code", "Madrid", "name", "Population"), 4500000,
                            Map.of("code", "Barcelona", "name", "Population"), 3000000)));
            MainTypeRead actual = reader.read();
            assertEquals(expected, actual);
        }
    }

}
