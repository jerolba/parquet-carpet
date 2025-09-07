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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetGeography;
import com.jerolba.carpet.annotation.ParquetGeometry;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.annotation.PrecisionScale;
import com.jerolba.carpet.writer.CarpetWriterCollectionThreeLevelTest.Category;

//TODO: how can we verify that is correct with out using carpet reader?
class CarpetWriterCollectionOneLevelTest {

    private final GeometryFactory geomFactory = new GeometryFactory();
    private final WKBWriter wkbWriter = new WKBWriter();

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

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void simpleStringCollection() throws IOException {

        record SimpleTypeCollection(String name, List<String> values) {
        }

        var rec = new SimpleTypeCollection("foo", List.of("foo", "bar"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void simpleStringAsEnumCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetEnum String> values) {
        }

        var rec = new SimpleTypeCollection("foo", List.of("FOO", "BAR"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }

        record AsEnum(String name, List<Category> values) {
        }

        var recEnum = new AsEnum("foo", List.of(Category.FOO, Category.BAR));
        try (var carpetReaderEnum = writerTest.getCarpetReader(AsEnum.class)) {
            assertEquals(recEnum, carpetReaderEnum.read());
        }
    }

    @Test
    void simpleEnumCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Category> values) {
        }

        var rec = new SimpleTypeCollection("foo", List.of(Category.FOO, Category.BAR));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void simpleBigDecimalCollection() throws IOException {

        record SimpleTypeCollection(String name, List<BigDecimal> values) {
        }

        var rec = new SimpleTypeCollection("foo", List.of(new BigDecimal("1.0"), new BigDecimal("2.0")));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE)
                .withDecimalConfig(6, 2);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            var expected = new SimpleTypeCollection("foo", List.of(new BigDecimal("1.00"), new BigDecimal("2.00")));
            assertEquals(expected, carpetReader.read());
        }
    }

    @Test
    void simpleBigDecimalAnnotatedCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@PrecisionScale(precision = 6, scale = 3) BigDecimal> values) {
        }

        var rec = new SimpleTypeCollection("foo", List.of(new BigDecimal("1.0"), new BigDecimal("2.0")));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            var expected = new SimpleTypeCollection("foo", List.of(new BigDecimal("1.000"), new BigDecimal("2.000")));
            assertEquals(expected, carpetReader.read());
        }
    }

    @Test
    void simpleJsonCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetJson String> values) {
        }

        var rec = new SimpleTypeCollection("foo",
                List.of("{\"key\": 1, \"value\": \"foo\"}", "{\"key\": 2, \"value\": \"bar\"}"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void simpleBinaryCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Binary> values) {
        }

        byte[] binary1 = new byte[] { 1, 2 };
        byte[] binary2 = new byte[] { 3, 4 };
        var rec = new SimpleTypeCollection("foo",
                List.of(Binary.fromConstantByteArray(binary1), Binary.fromConstantByteArray(binary2)));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void simpleBsonCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetBson Binary> values) {
        }

        byte[] mockBson1 = new byte[] { 1, 2 };
        byte[] mockBson2 = new byte[] { 3, 4 };
        var rec = new SimpleTypeCollection("foo",
                List.of(Binary.fromConstantByteArray(mockBson1), Binary.fromConstantByteArray(mockBson2)));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void simpleGeometryCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetGeometry Binary> values) {
        }

        Binary point1 = Binary
                .fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0))));
        Binary point2 = Binary
                .fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0))));

        var rec = new SimpleTypeCollection("foo", List.of(point1, point2));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void simpleGeographyCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetGeography Binary> values) {
        }

        Binary point1 = Binary
                .fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0))));
        Binary point2 = Binary
                .fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0))));

        var rec = new SimpleTypeCollection("foo", List.of(point1, point2));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
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

        try (var carpetReader = writerTest.getCarpetReader()) {
            EmptyCollection expectedNullList = new EmptyCollection("foo", null);
            assertEquals(expectedNullList, carpetReader.read());
        }
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

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
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

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
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

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

}
