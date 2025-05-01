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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.AnnotatedLevels;
import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.annotation.ParquetString;

class CarpetReaderToMapTest {

    enum Category {
        one, two, three;
    }

    @Nested
    class RootMap {

        @Test
        void convertRootGroupToMap() throws IOException {

            record MainTypeWrite(String name, Long code, int value) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite("root", 100L, 1);
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = Map.of("name", "root", "code", 100L, "value", 1);
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertRootGroupPrimitiveToMap() throws IOException {

            record MainTypeWrite(String id, byte asByte, short asShort, int asInt, long asLong, float asFloat,
                    double asDouble, boolean asBoolean, Category asEnum, UUID asUuid) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite("root", (byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true, Category.one,
                    UUID.randomUUID());
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = Map.of(
                    "id", "root",
                    "asByte", (byte) 1,
                    "asShort", (short) 2,
                    "asInt", 3,
                    "asLong", 4L,
                    "asFloat", 5.0f,
                    "asDouble", 6.0,
                    "asBoolean", true,
                    "asEnum", "one",
                    "asUuid", root.asUuid());
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertRootGroupObjectToMap() throws IOException {

            record MainTypeWrite(String id, LocalDate asLocalDate, LocalTime asLocalTime,
                    LocalDateTime asLocalDateTime, Instant asInstant, BigDecimal asBigDecimal) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withDecimalConfig(20, 2);
            var root = new MainTypeWrite("main", LocalDate.of(2024, 4, 28), LocalTime.of(18, 44, 28, 123456789),
                    LocalDateTime.of(2024, 4, 28, 18, 44, 28, 123456789),
                    LocalDateTime.of(2024, 4, 28, 18, 44, 28, 123456789).toInstant(ZoneOffset.ofHours(1)),
                    new BigDecimal("1234567.12"));
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = Map.of(
                    "id", "main",
                    "asLocalDate", LocalDate.of(2024, 4, 28),
                    "asLocalTime", LocalTime.of(18, 44, 28, 123000000),
                    "asLocalDateTime", LocalDateTime.of(2024, 4, 28, 18, 44, 28, 123000000),
                    "asInstant", LocalDateTime.of(2024, 4, 28, 18, 44, 28, 123000000).toInstant(ZoneOffset.ofHours(1)),
                    "asBigDecimal", new BigDecimal("1234567.12"));
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertRootGroupAnnotatedTypesToMap() throws IOException {

            record MainTypeWrite(String id, @ParquetEnum String category, @ParquetJson String json,
                    @ParquetString Category enumValue, @ParquetBson Binary bson, Binary binary) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite("root", "one", "{\"key\":\"value\"}", Category.two,
                    Binary.fromConstantByteArray(new byte[] { 1, 2, 3 }),
                    Binary.fromConstantByteArray(new byte[] { 4, 5, 6 }));
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = Map.of(
                    "id", "root",
                    "category", "one",
                    "json", "{\"key\":\"value\"}",
                    "enumValue", "two",
                    "bson", Binary.fromConstantByteArray(new byte[] { 1, 2, 3 }),
                    "binary", Binary.fromConstantByteArray(new byte[] { 4, 5, 6 }));
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertRootGroupNullValuesToMap() throws IOException {

            record MainTypeWrite(String id, Byte asByte, Short asShort, Integer asInt, Long asLong, Float asFloat,
                    Double asDouble, Boolean asBoolean, Category asEnum, UUID asUuid) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite(null, null, null, null, null, null, null, null, null, null);
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = new HashMap<>();
            expected.put("id", null);
            expected.put("asByte", null);
            expected.put("asShort", null);
            expected.put("asInt", null);
            expected.put("asLong", null);
            expected.put("asFloat", null);
            expected.put("asDouble", null);
            expected.put("asBoolean", null);
            expected.put("asEnum", null);
            expected.put("asUuid", null);
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertSingleLevelListValues() throws IOException {

            record MainTypeWrite(String id, List<String> values) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.ONE);
            var root = new MainTypeWrite("root", List.of("Madrid", "Barcelona"));
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = Map.of("id", "root", "values", List.of("Madrid", "Barcelona"));
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertTwoLevelListValues() throws IOException {

            record MainTypeWrite(String id, List<String> values) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.TWO);
            var root = new MainTypeWrite("root", List.of("Madrid", "Barcelona"));
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = Map.of("id", "root", "values", List.of("Madrid", "Barcelona"));
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertThreeLevelListValues() throws IOException {

            record MainTypeWrite(String id, List<String> values) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class)
                    .withLevel(AnnotatedLevels.THREE);
            var root = new MainTypeWrite("root", List.of("Madrid", "Barcelona"));
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = Map.of("id", "root", "values", List.of("Madrid", "Barcelona"));
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

        @Test
        void convertMapValues() throws IOException {

            record MainTypeWrite(String id, Map<String, Integer> values) {
            }

            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite("root", Map.of("Madrid", 120, "Barcelona", 230));
            writerTest.write(root);

            var reader = writerTest.getCarpetReader(Map.class);
            var expected = Map.of("id", "root", "values", Map.of("Madrid", 120, "Barcelona", 230));
            Map<String, Object> actual = reader.read();
            assertEquals(expected, actual);
        }

    }

    @Nested
    class NestedMap {

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
            void emptyListIsDeserializedAsNull() throws IOException {

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
                var expected = new MainTypeRead("root", mapOf("id", "foo", "values", null));
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
            void emptyListIsDeserializedAsEmpty() throws IOException {

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
                var expected = new MainTypeRead("root", mapOf("id", "foo", "values", List.of()));
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

    @Nested
    class MapClassSelection {

        record MainTypeWrite(String name, Long code, int value) {
        }

        @Nested
        class SelectTargetMap {

            private final Map<String, Object> expected = Map.of("name", "root", "code", 100L, "value", 1);

            private ParquetWriterTest<MainTypeWrite> writerTest;

            @BeforeEach
            void setup() throws IOException {
                writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
                var root = new MainTypeWrite("root", 100L, 1);
                writerTest.write(root);
            }

            @Test
            void genericMapIsConvertedToOptimizedCarpetGroupMap() throws IOException {
                var reader = writerTest.getCarpetReader(Map.class);

                Map<String, Object> actual = reader.read();
                assertEquals(expected, actual);
                assertTrue(actual.getClass().getName().contains("CarpetGroupMap"));
            }

            @Test
            void canConfigureHashMap() throws IOException {
                var reader = writerTest.getCarpetReader(HashMap.class);

                Map<String, Object> actual = reader.read();
                assertEquals(expected, actual);
                assertEquals(HashMap.class, actual.getClass());
            }

            @Test
            void canConfigureLinkedHashMap() throws IOException {
                var reader = writerTest.getCarpetReader(LinkedHashMap.class);

                Map<String, Object> actual = reader.read();
                assertEquals(expected, actual);
                assertEquals(LinkedHashMap.class, actual.getClass());
            }

            @Test
            void canConfigureTreeMap() throws IOException {
                var reader = writerTest.getCarpetReader(TreeMap.class);

                Map<String, Object> actual = reader.read();
                assertEquals(expected, actual);
                assertEquals(TreeMap.class, actual.getClass());
            }

        }

        @Nested
        class RecursiveRecordMapsUseSelectedMapType {

            private final Map<String, Object> expected = Map.of("name", "root", "code", 100L,
                    "value", Map.of("id", "foo", "amount", 1.0));

            record ChildType(String id, Double amount) {
            }

            record MainType(String name, Long code, ChildType value) {
            }

            private ParquetWriterTest<MainType> writerTest;

            @BeforeEach
            void setup() throws IOException {
                writerTest = new ParquetWriterTest<>(MainType.class);
                var root = new MainType("root", 100L, new ChildType("foo", 1.0));
                writerTest.write(root);
            }

            @Test
            void genericMapIsConvertedToOptimizedCarpetGroupMap() throws IOException {
                var reader = writerTest.getCarpetReader(Map.class);

                Map<String, Object> actual = reader.read();
                assertEquals(expected, actual);
                assertTrue(actual.getClass().getName().contains("CarpetGroupMap"));
                assertTrue(actual.get("value").getClass().getName().contains("CarpetGroupMap"));

            }

            @Test
            void canConfigureHashMap() throws IOException {
                var reader = writerTest.getCarpetReader(HashMap.class);

                Map<String, Object> actual = reader.read();
                assertEquals(expected, actual);
                assertEquals(HashMap.class, actual.getClass());
                assertEquals(HashMap.class, actual.get("value").getClass());
            }

        }

        @Test
        void carpetMapSupportsNullElements() throws IOException {
            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite(null, null, 1);
            writerTest.write(root);
            var reader = writerTest.getCarpetReader(Map.class);

            Map<String, Object> actual = reader.read();
            assertTrue(actual.containsKey("name"));
            assertNull(actual.get("name"));
            assertTrue(actual.containsKey("code"));
            assertNull(actual.get("code"));
            assertEquals(1, actual.get("value"));
        }

        @Test
        void hashMapDoesntContainNullElements() throws IOException {
            ParquetWriterTest<MainTypeWrite> writerTest = new ParquetWriterTest<>(MainTypeWrite.class);
            var root = new MainTypeWrite(null, null, 1);
            writerTest.write(root);
            var reader = writerTest.getCarpetReader(HashMap.class);

            Map<String, Object> actual = reader.read();
            assertFalse(actual.containsKey("name"));
            assertFalse(actual.containsKey("code"));
            assertEquals(1, actual.get("value"));
        }

    }

    private static Map<String, Object> mapOf(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }
}
