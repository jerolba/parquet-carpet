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
import static java.nio.file.Files.createTempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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

        String filePath = newTempFile("simpleRecord");
        try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
            writer.write(new Sample("A", 1, 2L));
        }

        List<String> classes = generateCode(filePath);
        assertTrue(classes.contains("record Sample(String a, int b, Long c) {}"));
    }

    @Test
    void primitiveTypes() throws IOException {

        record Sample(byte a, short b, int c, long d, float e, double f, boolean g) {
        }

        String filePath = newTempFile("primitiveTypes");
        try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
            writer.write(new Sample((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true));
        }

        List<String> classes = generateCode(filePath);
        assertTrue(classes.contains("record Sample(byte a, short b, int c, long d, float e, double f, boolean g) {}"));
    }

    @Test
    void dateAndTimeTypesMillis() throws IOException {

        record Sample(LocalDate localDate, LocalTime localTime, LocalDateTime localDateTime, Instant instant) {
        }

        String filePath = newTempFile("dateTimeTypesMillis");
        try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
            writer.write(new Sample(LocalDate.now(), LocalTime.now(), LocalDateTime.now(), Instant.now()));
        }

        List<String> classes = generateCode(filePath);
        assertTrue(classes.contains(
                "record Sample(LocalDate localDate, LocalTime localTime, LocalDateTime localDateTime, Instant instant) {}"));
    }

    @Test
    void dateAndTimeTypesNanos() throws IOException {

        record Sample(LocalDate localDate, LocalTime localTime, LocalDateTime localDateTime, Instant instant) {
        }

        String filePath = newTempFile("dateTimeTypesNanos");
        try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                .withDefaultTimeUnit(TimeUnit.NANOS).build()) {
            writer.write(new Sample(LocalDate.now(), LocalTime.now(), LocalDateTime.now(), Instant.now()));
        }

        List<String> classes = generateCode(filePath);
        assertTrue(classes.contains(
                "record Sample(LocalDate localDate, LocalTime localTime, LocalDateTime localDateTime, Instant instant) {}"));
    }

    @Test
    void objectTypes() throws IOException {

        enum FromEnum {
            A, B;
        }

        record Sample(Byte a, Short b, Integer c, Long d, Float e, Double f, Boolean g, String h, FromEnum i, UUID j,
                BigDecimal k) {
        }

        String filePath = newTempFile("objectTypes");
        try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                .withDefaultDecimal(10, 2).build()) {
            writer.write(new Sample((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true, "A", FromEnum.B, UUID.randomUUID(),
                    BigDecimal.TEN));
        }

        List<String> classes = generateCode(filePath);
        assertTrue(classes.contains(
                "record Sample(Byte a, Short b, Integer c, Long d, Float e, Double f, Boolean g, String h, String i, UUID j, BigDecimal k) {}"));
    }

    @Test
    void enumsAreDefinedAsStrings() throws IOException {

        enum FromEnum {
            A, B;
        }

        record Sample(String a, FromEnum b) {
        }

        String filePath = newTempFile("enumToString");
        try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
            writer.write(new Sample("A", FromEnum.B));
        }

        List<String> classes = generateCode(filePath);
        assertTrue(classes.contains("record Sample(String a, String b) {}"));
    }

    @Test
    void nestedRecordNameIsExtractedFromFieldName() throws IOException {

        record Child(byte d, Long e) {
        }
        record Sample(String a, int b, Child name) {
        }

        String filePath = newTempFile("nestedRecord");
        try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
            writer.write(new Sample("A", 1, new Child((byte) 1, 23L)));
        }

        List<String> classes = generateCode(filePath);
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
            String filePath = newTempFile("nestedList1");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.ONE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, int b, List<Long> c) {}"));
        }

        @Test
        void twoLevel() throws IOException {
            String filePath = newTempFile("nestedList2");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, int b, List<Long> c) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            String filePath = newTempFile("nestedList3");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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
            String filePath = newTempFile("nestedList1Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.ONE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, int b, List<Child> child) {}"));
            assertTrue(classes.contains("record Child(String d, double e) {}"));
        }

        @Test
        void twoLevel() throws IOException {
            String filePath = newTempFile("nestedList2Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, int b, List<Child> child) {}"));
            assertTrue(classes.contains("record Child(String d, double e) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            String filePath = newTempFile("nestedList3Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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
            String filePath = newTempFile("nestedList3Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.ONE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, int b, List<Map<String, Option>> option) {}"));
            assertTrue(classes.contains("record Option(String d, double e) {}"));
        }

        @Test
        void twoLevel() throws IOException {
            String filePath = newTempFile("nestedListMap2Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, int b, List<Map<String, Option>> option) {}"));
            assertTrue(classes.contains("record Option(String d, double e) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            String filePath = newTempFile("nestedListMap3Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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
            String filePath = newTempFile("nestedList2Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, int b, List<List<String>> child) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            String filePath = newTempFile("nestedList3Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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
            String filePath = newTempFile("nestedList2Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.TWO).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, int b, List<List<Child>> child) {}"));
            assertTrue(classes.contains("record Child(String d, double e) {}"));
        }

        @Test
        void threeLevel() throws IOException {
            String filePath = newTempFile("nestedList3Child");
            try (var writer = new CarpetWriter.Builder<>(new FileOutputStream(filePath), Sample.class)
                    .withLevelStructure(AnnotatedLevels.THREE).build()) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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

            String filePath = newTempFile("nestedMapSimple");
            try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, boolean b, Map<String, Integer> myMap) {}"));
        }

        @Test
        void simpleKeyAndRecordValue() throws IOException {

            record Child(String c, Long d) {
            }

            record Sample(String a, boolean b, Map<String, Child> cities) {
            }

            var value = new Sample("A", true, Map.of("B", new Child("Z", 12L), "C", new Child("Y", 23L)));

            String filePath = newTempFile("nestedMapRecordV");
            try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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

            String filePath = newTempFile("nestedMapRecordK");
            try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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

            String filePath = newTempFile("nestedMapListV");
            try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String a, boolean b, Map<String, List<Cities>> cities) {}"));
            assertTrue(classes.contains("record Cities(String c, Long d) {}"));
        }

        @Test
        void simpleKeyAndMapValue() throws IOException {

            record Sample(String a, int b, Map<String, Map<UUID, Integer>> cities) {
            }

            var value = new Sample("A", 1, Map.of("B", Map.of()));

            String filePath = newTempFile("nestedMapMapV");
            try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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

            String filePath = newTempFile("repeatedInSameLevel");
            try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
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

            String filePath = newTempFile("repeatedInDifferentSameLevel");
            try (var writer = new CarpetWriter<>(new FileOutputStream(filePath), Sample.class)) {
                writer.write(value);
            }
            List<String> classes = generateCode(filePath);
            assertTrue(classes.contains("record Sample(String id, A a, B b) {}"));
            assertTrue(classes.contains("record B(Long code, A d) {}"));
            assertTrue(classes.contains("record A(String c, float d) {}"));
            assertEquals(3, classes.size());
        }

    }

    private String newTempFile(String name) throws IOException {
        return createTempFile(name, ".parquet").toString();
    }

}
