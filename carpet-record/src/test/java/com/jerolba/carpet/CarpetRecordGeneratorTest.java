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

import static com.jerolba.carpet.CarpetRecordGenerator.generateCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CarpetRecordGeneratorTest {

    @Test
    void simpleRecord() throws IOException {

        record Sample(String a, int b, Long c) {
        }

        try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/simpleRecord.parquet"), Sample.class)) {
            writer.write(new Sample("A", 1, 2L));
        }

        List<String> classes = generateCode("/tmp/simpleRecord.parquet");
        assertTrue(classes.contains("record Sample(String a, int b, Long c) {}"));
    }

    @Test
    void primitiveTypes() throws IOException {

        record Sample(byte a, short b, int c, long d, float e, double f, boolean g) {
        }

        try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/primitiveTypes.parquet"), Sample.class)) {
            writer.write(new Sample((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true));
        }

        List<String> classes = generateCode("/tmp/primitiveTypes.parquet");
        assertTrue(classes.contains("record Sample(byte a, short b, int c, long d, float e, double f, boolean g) {}"));
    }

    @Test
    void objectTypes() throws IOException {

        enum FromEnum {
            A, B;
        }

        record Sample(Byte a, Short b, Integer c, Long d, Float e, Double f, Boolean g, String h, FromEnum i, UUID j) {
        }

        try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/objectTypes.parquet"), Sample.class)) {
            writer.write(new Sample((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true, "A", FromEnum.B, UUID.randomUUID()));
        }

        List<String> classes = generateCode("/tmp/objectTypes.parquet");
        assertTrue(classes.contains(
                "record Sample(Byte a, Short b, Integer c, Long d, Float e, Double f, Boolean g, String h, String i, UUID j) {}"));
    }

    @Test
    void enumsAreDefinedAsStrings() throws IOException {

        enum FromEnum {
            A, B;
        }

        record Sample(String a, FromEnum b) {
        }

        try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/enumToString.parquet"), Sample.class)) {
            writer.write(new Sample("A", FromEnum.B));
        }

        List<String> classes = generateCode("/tmp/enumToString.parquet");
        assertTrue(classes.contains("record Sample(String a, String b) {}"));
    }

    @Test
    void nestedRecordNameIsExtractedFromFieldName() throws IOException {

        record Child(byte d, Long e) {
        }
        record Sample(String a, int b, Child name) {
        }

        try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/nestedRecord.parquet"), Sample.class)) {
            writer.write(new Sample("A", 1, new Child((byte) 1, 23L)));
        }

        List<String> classes = generateCode("/tmp/nestedRecord.parquet");
        assertTrue(classes.contains("record Name(byte d, Long e) {}"));
        assertTrue(classes.contains("record Sample(String a, int b, Name name) {}"));
    }

    @Nested
    class NestedListSimpleChild {

        record Sample(String a, int b, List<Long> c) {
        }

        Sample value = new Sample("A", 1, List.of(2L));

        @Test
        void oneLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream("/tmp/nestedList1.parquet"),
                    Sample.class).levelStructure(AnnotatedLevels.ONE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList1.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Long> c) {}"));
        }

        @Test
        void twoLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream("/tmp/nestedList2.parquet"),
                    Sample.class).levelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList2.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Long> c) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream("/tmp/nestedList3.parquet"),
                    Sample.class).levelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList3.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Long> c) {}"));
        }
    }

    @Nested
    class NestedListWithRecord {

        record Child(String d, double e) {
        }

        record Sample(String a, int b, List<Child> child) {
        }

        Sample value = new Sample("A", 1, List.of(new Child("B", 1.2)));

        @Test
        void oneLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedList1Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.ONE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList1Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Child> child) {}"));
            assertTrue(classes.contains("record Child(String d, double e) {}"));
        }

        @Test
        void twoLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedList2Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList2Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Child> child) {}"));
            assertTrue(classes.contains("record Child(String d, double e) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedList3Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList3Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Child> child) {}"));
            assertTrue(classes.contains("record Child(String d, double e) {}"));
        }
    }

    @Nested
    class NestedListWithMap {

        record Child(String d, double e) {
        }

        record Sample(String a, int b, List<Map<String, Child>> option) {
        }

        Sample value = new Sample("A", 1, List.of(Map.of("foo", new Child("bar", 3))));

        @Test
        void oneLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedListMap1Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.ONE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedListMap1Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Map<String, Option>> option) {}"));
            assertTrue(classes.contains("record Option(String d, double e) {}"));
        }

        @Test
        void twoLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedListMap2Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedListMap2Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Map<String, Option>> option) {}"));
            assertTrue(classes.contains("record Option(String d, double e) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedListMap3Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedListMap3Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<Map<String, Option>> option) {}"));
            assertTrue(classes.contains("record Option(String d, double e) {}"));
        }
    }

    @Nested
    class DoubleNestedListWithSimple {

        record Sample(String a, int b, List<List<String>> child) {
        }

        Sample value = new Sample("A", 1, List.of(List.of("B")));

        @Test
        void twoLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedList2Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList2Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<List<String>> child) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedList3Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList3Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<List<String>> child) {}"));
        }
    }

    @Nested
    class DoubleNestedListWithRecord {

        record Child(String d, double e) {
        }

        record Sample(String a, int b, List<List<Child>> child) {
        }

        Sample value = new Sample("A", 1, List.of(List.of(new Child("B", 1.2))));

        @Test
        void twoLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedList2Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList2Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<List<Child>> child) {}"));
            assertTrue(classes.contains("record Child(String d, double e) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            try (var writer = new CarpetWriter.Builder<>(
                    new FileOutputStream("/tmp/nestedList3Child.parquet"), Sample.class)
                            .levelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedList3Child.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, List<List<Child>> child) {}"));
            assertTrue(classes.contains("record Child(String d, double e) {}"));
        }
    }

    @Nested
    class NestedMap {

        @Test
        void simpleKeyAndValue() throws IOException {

            record Sample(String a, boolean b, Map<String, Integer> myMap) {
            }

            var value = new Sample("A", true, Map.of("B", 1, "C", 2));

            try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/nestedMapSimple.parquet"), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedMapSimple.parquet");
            assertTrue(classes.contains("record Sample(String a, boolean b, Map<String, Integer> myMap) {}"));
        }

        @Test
        void simpleKeyAndRecordValue() throws IOException {

            record Child(String c, Long d) {
            }

            record Sample(String a, boolean b, Map<String, Child> cities) {
            }

            var value = new Sample("A", true, Map.of("B", new Child("Z", 12L), "C", new Child("Y", 23L)));

            try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/nestedMapRecordV.parquet"), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedMapRecordV.parquet");
            assertTrue(classes.contains("record Sample(String a, boolean b, Map<String, Cities> cities) {}"));
            assertTrue(classes.contains("record Cities(String c, Long d) {}"));
        }

        @Test
        void recordKeyAndSimpleValue() throws IOException {

            record Child(String c, Long d) {
            }

            record Sample(String a, boolean b, Map<Child, String> cities) {
            }

            var value = new Sample("A", true, Map.of(new Child("Z", 12L), "B", new Child("Y", 23L), "C"));

            try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/nestedMapRecordK.parquet"), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedMapRecordK.parquet");
            assertTrue(classes.contains("record Sample(String a, boolean b, Map<CitiesKey, String> cities) {}"));
            assertTrue(classes.contains("record CitiesKey(String c, Long d) {}"));
        }

        @Test
        void simpleKeyAndListValue() throws IOException {

            record Child(String c, Long d) {
            }

            record Sample(String a, boolean b, Map<String, List<Child>> cities) {
            }

            var value = new Sample("A", true, Map.of("B", List.of(new Child("Z", 12L), new Child("Y", 23L))));

            try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/nestedMapListV.parquet"), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedMapListV.parquet");
            assertTrue(classes.contains("record Sample(String a, boolean b, Map<String, List<Cities>> cities) {}"));
            assertTrue(classes.contains("record Cities(String c, Long d) {}"));
        }

        @Test
        void simpleKeyAndMapValue() throws IOException {

            record Sample(String a, int b, Map<String, Map<UUID, Integer>> cities) {
            }

            var value = new Sample("A", 1, Map.of("B", Map.of()));

            try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/nestedMapMapV.parquet"), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/nestedMapMapV.parquet");
            assertTrue(classes.contains("record Sample(String a, int b, Map<String, Map<UUID, Integer>> cities) {}"));
        }

    }

    @Nested
    class RepeatedRecords {

        @Test
        void repeatedInSameLevel() throws IOException {

            record Child(String c, float d) {
            }

            record Sample(String id, Child a, Child b) {
            }

            var value = new Sample("foo", new Child("a", 1.2f), new Child("b", 2.2f));

            try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/repeatedInSameLevel.parquet"),
                    Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/repeatedInSameLevel.parquet");
            assertTrue(classes.contains("record Sample(String id, A a, A b) {}"));
            assertTrue(classes.contains("record A(String c, float d) {}"));
            assertEquals(2, classes.size());
        }

        @Test
        void repeatedInDifferentSameLevel() throws IOException {

            record Child(String c, float d) {
            }

            record Data(Long code, Child d) {
            }

            record Sample(String id, Child a, Data b) {
            }

            var value = new Sample("foo", new Child("a", 1.2f), new Data(1L, new Child("b", 2.2f)));

            try (var writer = new CarpetWriter<>(new FileOutputStream("/tmp/repeatedInDifferentSameLevel.parquet"),
                    Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode("/tmp/repeatedInDifferentSameLevel.parquet");
            assertTrue(classes.contains("record Sample(String id, A a, B b) {}"));
            assertTrue(classes.contains("record B(Long code, A d) {}"));
            assertTrue(classes.contains("record A(String c, float d) {}"));
            assertEquals(3, classes.size());
        }

    }

}
