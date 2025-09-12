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

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;

import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetGeography;
import com.jerolba.carpet.annotation.ParquetGeometry;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.annotation.PrecisionScale;

class CarpetWriterMapTest {

    private final GeometryFactory geomFactory = new GeometryFactory();
    private final WKBWriter wkbWriter = new WKBWriter();

    enum Category {
        FOO, BAR
    }

    @Test
    void mapPrimitiveValue() throws IOException {

        record MapPrimitiveValue(String name, Map<String, Integer> ids, Map<String, Double> amount) {
        }

        var rec1 = new MapPrimitiveValue("foo", mapOf("ABCD", 1, "EFGH", 2), mapOf("ABCD", 1.2, "EFGH", 2.3));
        var rec2 = new MapPrimitiveValue("bar", mapOf("ABCD", 3, "EFGH", 4), mapOf("ABCD", 2.2, "EFGH", 3.3));
        var writerTest = new ParquetWriterTest<>(MapPrimitiveValue.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            assertEquals(rec1.ids(), unUtf8Map(avroRecord.get("ids")));
            assertEquals(rec1.amount(), unUtf8Map(avroRecord.get("amount")));

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            assertEquals(rec2.ids(), unUtf8Map(avroRecord.get("ids")));
            assertEquals(rec2.amount(), unUtf8Map(avroRecord.get("amount")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapPrimitiveValueNull() throws IOException {

        record MapPrimitiveValueNull(String name, Map<String, Integer> ids, Map<String, Double> amount) {
        }

        var rec1 = new MapPrimitiveValueNull("foo", mapOf("ABCD", 1, "EFGH", 2), mapOf("ABCD", 1.2, "EFGH", 2.3));
        var rec2 = new MapPrimitiveValueNull("bar", mapOf("ABCD", null, "EFGH", 4), mapOf("ABCD", 2.2, "EFGH", null));
        var writerTest = new ParquetWriterTest<>(MapPrimitiveValueNull.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            assertEquals(rec1.ids(), unUtf8Map(avroRecord.get("ids")));
            assertEquals(rec1.amount(), unUtf8Map(avroRecord.get("amount")));

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            assertEquals(rec2.ids(), unUtf8Map(avroRecord.get("ids")));
            assertEquals(rec2.amount(), unUtf8Map(avroRecord.get("amount")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapStringKeyAndValue() throws IOException {

        record MapValues(String name, Map<String, String> values) {
        }

        var rec1 = new MapValues("foo", Map.of("ABCD", "ONE", "EFGH", "TWO"));
        var rec2 = new MapValues("bar", Map.of("ABCD", "THREE", "EFGH", "FOUR"));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            assertEquals(rec1.values(), unUtf8Map(avroRecord.get("values")));

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            assertEquals(rec2.values(), unUtf8Map(avroRecord.get("values")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapStringAsEnumValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetEnum String> values) {
        }

        var rec1 = new MapValues("foo", Map.of("FOO", "BAR", "BAR", "FOO"));
        var rec2 = new MapValues("bar", Map.of("FOO", "FOO", "BAR", "BAR"));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            assertEquals(rec1.values(), unUtf8Map(avroRecord.get("values")));

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            assertEquals(rec2.values(), unUtf8Map(avroRecord.get("values")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        record MapValuesAsEnum(String name, Map<String, Category> values) {
        }

        try (var carpetReaderAsEnum = writerTest.getCarpetReader(MapValuesAsEnum.class)) {
            assertEquals(new MapValuesAsEnum("foo", Map.of("FOO", Category.BAR, "BAR", Category.FOO)),
                    carpetReaderAsEnum.read());
            assertEquals(new MapValuesAsEnum("bar", Map.of("FOO", Category.FOO, "BAR", Category.BAR)),
                    carpetReaderAsEnum.read());
        }
    }

    @Test
    void mapEnumValue() throws IOException {

        record MapValues(String name, Map<String, Category> values) {
        }

        var rec1 = new MapValues("foo", Map.of("FOO", Category.BAR, "BAR", Category.FOO));
        var rec2 = new MapValues("bar", Map.of("FOO", Category.FOO, "BAR", Category.BAR));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            assertEquals(Map.of("FOO", "BAR", "BAR", "FOO"), unUtf8Map(avroRecord.get("values")));

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            assertEquals(Map.of("FOO", "FOO", "BAR", "BAR"), unUtf8Map(avroRecord.get("values")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapBigDecimalValue() throws IOException {

        record MapValues(String name, Map<String, BigDecimal> values) {
        }

        var rec = new MapValues("foo", Map.of("FOO", new BigDecimal("2.0"), "BAR", new BigDecimal("6.0")));
        var writerTest = new ParquetWriterTest<>(MapValues.class)
                .withDecimalConfig(20, 2);
        writerTest.write(rec);

        GenericData genericDataModel = new GenericData();
        genericDataModel.addLogicalTypeConversion(new DecimalConversion());
        try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals("foo", avroRecord.get("name").toString());
            assertEquals(Map.of("FOO", new BigDecimal("2.00"), "BAR", new BigDecimal("6.00")),
                    unUtf8Map(avroRecord.get("values")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            MapValues carpetRecord = carpetReader.read();
            assertEquals("foo", carpetRecord.name());
            assertEquals(Map.of("FOO", new BigDecimal("2.00"), "BAR", new BigDecimal("6.00")), carpetRecord.values());
        }
    }

    @Test
    void mapBigDecimalAnnotatedValue() throws IOException {

        record MapValues(String name, Map<String, @PrecisionScale(precision = 20, scale = 3) BigDecimal> values) {
        }

        var rec = new MapValues("foo", Map.of("FOO", new BigDecimal("2.0"), "BAR", new BigDecimal("6.0")));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec);

        GenericData genericDataModel = new GenericData();
        genericDataModel.addLogicalTypeConversion(new DecimalConversion());
        try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals("foo", avroRecord.get("name").toString());
            assertEquals(Map.of("FOO", new BigDecimal("2.000"), "BAR", new BigDecimal("6.000")),
                    unUtf8Map(avroRecord.get("values")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            MapValues carpetRecord = carpetReader.read();
            assertEquals("foo", carpetRecord.name());
            assertEquals(Map.of("FOO", new BigDecimal("2.000"), "BAR", new BigDecimal("6.000")), carpetRecord.values());
        }
    }

    @Test
    void mapJsonValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetJson String> values) {
        }

        var rec1 = new MapValues("foo", Map.of("FOO", "{}", "BAR", "{\"key1\": \"value1\"}"));
        var rec2 = new MapValues("bar", Map.of("FOO", "{\"key2\": \"value2\"}", "BAR", "{}"));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            assertEquals(rec1.values(), unByteBufferMap(avroRecord.get("values")));

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            assertEquals(rec2.values(), unByteBufferMap(avroRecord.get("values")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapBinaryValue() throws IOException {

        record MapValues(String name, Map<String, Binary> values) {
        }

        var rec1 = new MapValues("foo", Map.of("FOO", Binary.fromString("BAR"), "BAR", Binary.fromString("FOO")));
        var rec2 = new MapValues("bar", Map.of("FOO", Binary.fromString("FOO"), "BAR", Binary.fromString("BAR")));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            assertEquals(Map.of("FOO", "BAR", "BAR", "FOO"), unByteBufferMap(avroRecord.get("values")));

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            assertEquals(Map.of("FOO", "FOO", "BAR", "BAR"), unByteBufferMap(avroRecord.get("values")));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapBsonValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetBson Binary> values) {
        }

        byte[] mockBson11 = new byte[] { 1, 2 };
        byte[] mockBson12 = new byte[] { 3, 4 };
        byte[] mockBson21 = new byte[] { 5, 6 };
        byte[] mockBson22 = new byte[] { 7, 8 };

        var rec1 = new MapValues("foo", Map.of("FOO", Binary.fromConstantByteArray(mockBson11),
                "BAR", Binary.fromConstantByteArray(mockBson12)));
        var rec2 = new MapValues("bar", Map.of("FOO", Binary.fromConstantByteArray(mockBson21),
                "BAR", Binary.fromConstantByteArray(mockBson22)));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            Map<Object, Object> map1 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map1.containsKey("FOO"));
            assertBinaryEquals(mockBson11, ((ByteBuffer) map1.get("FOO")).array());
            assertTrue(map1.containsKey("BAR"));
            assertBinaryEquals(mockBson12, ((ByteBuffer) map1.get("BAR")).array());

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            Map<Object, Object> map2 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map2.containsKey("FOO"));
            assertBinaryEquals(mockBson21, ((ByteBuffer) map2.get("FOO")).array());
            assertTrue(map2.containsKey("BAR"));
            assertBinaryEquals(mockBson22, ((ByteBuffer) map2.get("BAR")).array());
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapGeometryValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetGeometry Binary> values) {
        }
        byte[] geo11 = wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0)));
        byte[] geo12 = wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 2.0)));
        byte[] geo21 = wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 1.0)));
        byte[] geo22 = wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0)));

        var rec1 = new MapValues("foo", Map.of("FOO", Binary.fromConstantByteArray(geo11),
                "BAR", Binary.fromConstantByteArray(geo12)));
        var rec2 = new MapValues("bar", Map.of("FOO", Binary.fromConstantByteArray(geo21),
                "BAR", Binary.fromConstantByteArray(geo22)));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            Map<Object, Object> map1 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map1.containsKey("FOO"));
            assertBinaryEquals(geo11, ((ByteBuffer) map1.get("FOO")).array());
            assertTrue(map1.containsKey("BAR"));
            assertBinaryEquals(geo12, ((ByteBuffer) map1.get("BAR")).array());

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            Map<Object, Object> map2 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map2.containsKey("FOO"));
            assertBinaryEquals(geo21, ((ByteBuffer) map2.get("FOO")).array());
            assertTrue(map2.containsKey("BAR"));
            assertBinaryEquals(geo22, ((ByteBuffer) map2.get("BAR")).array());
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapGeometryJtsValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetGeometry Geometry> values) {
        }
        Point geo11 = geomFactory.createPoint(new Coordinate(1.0, 1.0));
        Point geo12 = geomFactory.createPoint(new Coordinate(1.0, 2.0));
        Point geo21 = geomFactory.createPoint(new Coordinate(2.0, 1.0));
        Point geo22 = geomFactory.createPoint(new Coordinate(2.0, 2.0));

        var rec1 = new MapValues("foo", Map.of("FOO", geo11, "BAR", geo12));
        var rec2 = new MapValues("bar", Map.of("FOO", geo21, "BAR", geo22));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            Map<Object, Object> map1 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map1.containsKey("FOO"));
            assertGoemetryEquals(geo11, ((ByteBuffer) map1.get("FOO")).array());
            assertTrue(map1.containsKey("BAR"));
            assertGoemetryEquals(geo12, ((ByteBuffer) map1.get("BAR")).array());

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            Map<Object, Object> map2 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map2.containsKey("FOO"));
            assertGoemetryEquals(geo21, ((ByteBuffer) map2.get("FOO")).array());
            assertTrue(map2.containsKey("BAR"));
            assertGoemetryEquals(geo22, ((ByteBuffer) map2.get("BAR")).array());
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapGeographyValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetGeography Binary> values) {
        }
        byte[] geo11 = wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0)));
        byte[] geo12 = wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 2.0)));
        byte[] geo21 = wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 1.0)));
        byte[] geo22 = wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0)));

        var rec1 = new MapValues("foo", Map.of("FOO", Binary.fromConstantByteArray(geo11),
                "BAR", Binary.fromConstantByteArray(geo12)));
        var rec2 = new MapValues("bar", Map.of("FOO", Binary.fromConstantByteArray(geo21),
                "BAR", Binary.fromConstantByteArray(geo22)));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            Map<Object, Object> map1 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map1.containsKey("FOO"));
            assertBinaryEquals(geo11, ((ByteBuffer) map1.get("FOO")).array());
            assertTrue(map1.containsKey("BAR"));
            assertBinaryEquals(geo12, ((ByteBuffer) map1.get("BAR")).array());

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            Map<Object, Object> map2 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map2.containsKey("FOO"));
            assertBinaryEquals(geo21, ((ByteBuffer) map2.get("FOO")).array());
            assertTrue(map2.containsKey("BAR"));
            assertBinaryEquals(geo22, ((ByteBuffer) map2.get("BAR")).array());
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapGeographyJtsValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetGeography Geometry> values) {
        }
        Point geo11 = geomFactory.createPoint(new Coordinate(1.0, 1.0));
        Point geo12 = geomFactory.createPoint(new Coordinate(1.0, 2.0));
        Point geo21 = geomFactory.createPoint(new Coordinate(2.0, 1.0));
        Point geo22 = geomFactory.createPoint(new Coordinate(2.0, 2.0));

        var rec1 = new MapValues("foo", Map.of("FOO", geo11, "BAR", geo12));
        var rec2 = new MapValues("bar", Map.of("FOO", geo21, "BAR", geo22));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            Map<Object, Object> map1 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map1.containsKey("FOO"));
            assertGoemetryEquals(geo11, ((ByteBuffer) map1.get("FOO")).array());
            assertTrue(map1.containsKey("BAR"));
            assertGoemetryEquals(geo12, ((ByteBuffer) map1.get("BAR")).array());

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            Map<Object, Object> map2 = (Map<Object, Object>) byteBufferMap(avroRecord.get("values"));
            assertTrue(map2.containsKey("FOO"));
            assertGoemetryEquals(geo21, ((ByteBuffer) map2.get("FOO")).array());
            assertTrue(map2.containsKey("BAR"));
            assertGoemetryEquals(geo22, ((ByteBuffer) map2.get("BAR")).array());
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    private static void assertBinaryEquals(byte[] expected, byte[] actualBytes) {
        assertEquals(expected.length, actualBytes.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actualBytes[i]);
        }
    }

    private static void assertGoemetryEquals(Geometry expectedGeo, byte[] actualBytes) {
        byte[] expected = new WKBWriter().write(expectedGeo);
        assertEquals(expected.length, actualBytes.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actualBytes[i]);
        }
    }

    @Test
    void mapRecordValue() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record MapRecordValue(String name, Map<String, ChildRecord> ids) {
        }

        var rec1 = new MapRecordValue("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                "ZGZ", new ChildRecord("Zaragoza", false)));
        var rec2 = new MapRecordValue("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", false),
                "CAT", new ChildRecord("Barcelona", true)));
        var writerTest = new ParquetWriterTest<>(MapRecordValue.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals("AVE", avroRecord.get("name").toString());
            var idsMap = (Map<String, GenericRecord>) unUtf8Map(avroRecord.get("ids"));
            assertEquals("Madrid", idsMap.get("MAD").get("id").toString());
            assertEquals(true, idsMap.get("MAD").get("active"));
            assertEquals("Zaragoza", idsMap.get("ZGZ").get("id").toString());
            assertEquals(false, idsMap.get("ZGZ").get("active"));

            avroRecord = avroReader.read();
            assertEquals("AVE", avroRecord.get("name").toString());
            idsMap = (Map<String, GenericRecord>) unUtf8Map(avroRecord.get("ids"));
            assertEquals("Madrid", idsMap.get("MAD").get("id").toString());
            assertEquals(false, idsMap.get("MAD").get("active"));
            assertEquals("Barcelona", idsMap.get("CAT").get("id").toString());
            assertEquals(true, idsMap.get("CAT").get("active"));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapRecordValueNull() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record MapRecordValueNull(String name, Map<String, ChildRecord> ids) {
        }

        var rec1 = new MapRecordValueNull("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                "ZGZ", new ChildRecord("Zaragoza", false)));
        var rec2 = new MapRecordValueNull("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                "BDJ", null));

        var writerTest = new ParquetWriterTest<>(MapRecordValueNull.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            var idsMap = (Map<String, GenericRecord>) unUtf8Map(avroRecord.get("ids"));
            assertEquals("Madrid", idsMap.get("MAD").get("id").toString());
            assertEquals(true, idsMap.get("MAD").get("active"));
            assertEquals("Zaragoza", idsMap.get("ZGZ").get("id").toString());
            assertEquals(false, idsMap.get("ZGZ").get("active"));

            avroRecord = avroReader.read();
            assertEquals(rec2.name(), avroRecord.get("name").toString());
            idsMap = (Map<String, GenericRecord>) unUtf8Map(avroRecord.get("ids"));
            assertEquals("Madrid", idsMap.get("MAD").get("id").toString());
            assertEquals(true, idsMap.get("MAD").get("active"));
            assertNull(idsMap.get("BDJ"));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void mapRecordKeyNull() {

        record ChildRecord(String id, boolean active) {
        }
        record MapRecordValueNull(String name, Map<String, ChildRecord> ids) {
        }

        var rec1 = new MapRecordValueNull("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                null, new ChildRecord("Heaven", false)));

        var writerTest = new ParquetWriterTest<>(MapRecordValueNull.class);
        assertThrows(InvalidRecordException.class, () -> writerTest.write(rec1));
    }

    @Test
    void nestedMap_MapPrimitiveValue() throws IOException {

        record NestedMap_MapPrimitiveValue(String id, Map<String, Map<String, Integer>> values) {
        }

        var rec1 = new NestedMap_MapPrimitiveValue("Plane", mapOf(
                "ABCD", mapOf("EFGH", 10, "IJKL", 20),
                "WXYZ", mapOf("EFGH", 30, "IJKL", 50)));
        var rec2 = new NestedMap_MapPrimitiveValue("Boat", mapOf(
                "ABCD", mapOf("EFGH", 40, "IJKL", 50),
                "WXYZ", mapOf("EFGH", 70, "IJKL", 90)));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapPrimitiveValue.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.id(), avroRecord.get("id").toString());
            var values = (Map<String, Map<String, Integer>>) unUtf8Map(avroRecord.get("values"));
            assertEquals(rec1.values, values);
            avroRecord = avroReader.read();
            assertEquals(rec2.id(), avroRecord.get("id").toString());
            values = (Map<String, Map<String, Integer>>) unUtf8Map(avroRecord.get("values"));
            assertEquals(rec2.values, values);
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void nestedMap_MapPrimitiveValueNull() throws IOException {

        record NestedMap_MapPrimitiveValueNull(String id, Map<String, Map<String, Integer>> values) {
        }

        var rec1 = new NestedMap_MapPrimitiveValueNull("Plane", mapOf(
                "ABCD", mapOf("EFGH", 10, "IJKL", 20),
                "WXYZ", mapOf("EFGH", 30, "IJKL", 50)));
        var rec2 = new NestedMap_MapPrimitiveValueNull("Boat", mapOf(
                "ABCD", mapOf("EFGH", 40, "IJKL", null),
                "WXYZ", null));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapPrimitiveValueNull.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.id(), avroRecord.get("id").toString());
            var values = (Map<String, Map<String, Integer>>) unUtf8Map(avroRecord.get("values"));
            assertEquals(rec1.values, values);
            avroRecord = avroReader.read();
            assertEquals(rec2.id(), avroRecord.get("id").toString());
            values = (Map<String, Map<String, Integer>>) unUtf8Map(avroRecord.get("values"));
            assertEquals(rec2.values, values);
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void nestedMap_MapRecordValue() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record NestedMap_MapRecordValue(String name, Map<String, Map<String, ChildRecord>> ids) {
        }

        var rec1 = new NestedMap_MapRecordValue("Trip", mapOf(
                "AA", mapOf("FF", new ChildRecord("Madrid", true), "200", new ChildRecord("Sevilla", false)),
                "BB", mapOf("JJ", new ChildRecord("Bilbao", false), "300", new ChildRecord("Zaragoza", false))));
        var rec2 = new NestedMap_MapRecordValue("Hotel", mapOf(
                "ZZ", mapOf("100", new ChildRecord("Madrid", true), "200", new ChildRecord("Sevilla", false)),
                "YY", mapOf("200", new ChildRecord("Bilbao", false), "300", new ChildRecord("Zaragoza", false))));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapRecordValue.class);
        writerTest.write(rec1, rec2);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec1.name(), avroRecord.get("name").toString());
            var ids = (Map<String, Map<String, GenericRecord>>) unUtf8Map(avroRecord.get("ids"));
            assertEquals("Madrid", ids.get("AA").get("FF").get("id").toString());
            assertEquals(true, ids.get("AA").get("FF").get("active"));
            assertEquals("Sevilla", ids.get("AA").get("200").get("id").toString());
            assertEquals(false, ids.get("AA").get("200").get("active"));
            assertEquals("Bilbao", ids.get("BB").get("JJ").get("id").toString());
            assertEquals(false, ids.get("BB").get("JJ").get("active"));
            assertEquals("Zaragoza", ids.get("BB").get("300").get("id").toString());
            assertEquals(false, ids.get("BB").get("300").get("active"));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void nestedMap_MapRecordValueNull() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record NestedMap_MapRecordValueNull(String name, Map<String, Map<String, ChildRecord>> ids) {
        }

        var rec = new NestedMap_MapRecordValueNull("Hotel", mapOf(
                "ZZ", mapOf("100", new ChildRecord("Madrid", true), "200", null),
                "YY", null));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapRecordValueNull.class);
        writerTest.write(rec);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec.name(), avroRecord.get("name").toString());
            var ids = (Map<String, Map<String, GenericRecord>>) unUtf8Map(avroRecord.get("ids"));
            assertEquals("Madrid", ids.get("ZZ").get("100").get("id").toString());
            assertEquals(true, ids.get("ZZ").get("100").get("active"));
            assertNull(ids.get("ZZ").get("200"));
            assertNull(ids.get("YY"));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void nestedMap_Record_Map() throws IOException {

        record ChildWithMap(String name, Map<String, Integer> alias) {
        }
        record NestedMap_Record_Map(String id, Map<String, ChildWithMap> values) {
        }

        var rec = new NestedMap_Record_Map("foo", mapOf(
                "OS", new ChildWithMap("Apple", mapOf("MacOs", 1000, "OS X", 2000)),
                "CP", new ChildWithMap("MS", mapOf("Windows 10", 33, "Windows 11", 54))));
        var writerTest = new ParquetWriterTest<>(NestedMap_Record_Map.class);
        writerTest.write(rec);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec.id(), avroRecord.get("id").toString());
            var values = (Map<String, GenericRecord>) unUtf8Map(avroRecord.get("values"));
            GenericRecord child1 = values.get("OS");
            assertEquals(rec.values.get("OS").name, child1.get("name").toString());
            var alias1 = (Map<String, GenericRecord>) unUtf8Map(child1.get("alias"));
            assertEquals(rec.values.get("OS").alias, alias1);
            GenericRecord child2 = values.get("CP");
            assertEquals(rec.values.get("CP").name, child2.get("name").toString());
            var alias2 = (Map<String, GenericRecord>) unUtf8Map(child2.get("alias"));
            assertEquals(rec.values.get("CP").alias, alias2);
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void mapKeyRecord() throws IOException {

        record KeyRecord(String id, boolean active) {
        }
        record MapKeyRecord(String name, Map<KeyRecord, String> ids) {
        }

        var rec = new MapKeyRecord("foo", mapOf(
                new KeyRecord("Madrid", true), "MAD",
                new KeyRecord("Barcelona", true), "BCN"));
        var writerTest = new ParquetWriterTest<>(MapKeyRecord.class);
        writerTest.write(rec);

        // Avro reader doesn't support Maps with groups as keys.
        String expected = """
                message MapKeyRecord {
                  optional binary name (STRING);
                  optional group ids (MAP) {
                    repeated group key_value {
                      required group key {
                        optional binary id (STRING);
                        required boolean active;
                      }
                      optional binary value (STRING);
                    }
                  }
                }
                """;
        assertEquals(expected, writerTest.getSchema().toString());

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void mapKeyRecord_ValueRecord() throws IOException {

        record KeyRecord(String id, boolean active) {
        }
        record ValueRecord(String id, int age) {
        }
        record MapKeyAndValueRecord(String name, Map<KeyRecord, ValueRecord> ids) {
        }

        var rec = new MapKeyAndValueRecord("Time", mapOf(
                new KeyRecord("Madrid", true), new ValueRecord("MAD", 10),
                new KeyRecord("Barcelona", true), new ValueRecord("MAD", 10)));
        var writerTest = new ParquetWriterTest<>(MapKeyAndValueRecord.class);
        writerTest.write(rec);

        // Avro reader doesn't support Maps with groups as keys.
        String expected = """
                message MapKeyAndValueRecord {
                  optional binary name (STRING);
                  optional group ids (MAP) {
                    repeated group key_value {
                      required group key {
                        optional binary id (STRING);
                        required boolean active;
                      }
                      optional group value {
                        optional binary id (STRING);
                        required int32 age;
                      }
                    }
                  }
                }
                """;
        assertEquals(expected, writerTest.getSchema().toString());

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void emptyMapIsTransformedToEmptyMap() throws IOException {

        record EmptyMap(String name, Map<String, Integer> ids) {
        }

        var rec = new EmptyMap("foo", Map.of());
        var writerTest = new ParquetWriterTest<>(EmptyMap.class);
        writerTest.write(rec);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord avroRecord = avroReader.read();
            assertEquals(rec.name(), avroRecord.get("name").toString());
            assertEquals(emptyMap(), avroRecord.get("ids"));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            EmptyMap expectedEmptyMap = new EmptyMap("foo", emptyMap());
            assertEquals(expectedEmptyMap, carpetReader.read());
        }
    }

    @Test
    void emptyNestedMapIsSupported() throws IOException {

        record EmptyNestedMap(String name, Map<String, Map<String, Integer>> ids) {
        }

        var rec = new EmptyNestedMap("foo", Map.of("key", Map.of()));
        var writerTest = new ParquetWriterTest<>(EmptyNestedMap.class);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            EmptyNestedMap expectedEmptyMap = new EmptyNestedMap("foo", Map.of("key", emptyMap()));
            assertEquals(expectedEmptyMap, carpetReader.read());
        }
    }

    @Test
    void nullNestedMapIsSupported() throws IOException {

        record EmptyNestedMap(String name, Map<String, Map<String, Integer>> ids) {
        }

        Map<String, Map<String, Integer>> ids = new HashMap<>();
        ids.put("key", null);
        var rec = new EmptyNestedMap("foo", ids);
        var writerTest = new ParquetWriterTest<>(EmptyNestedMap.class);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            EmptyNestedMap expectedEmptyMap = new EmptyNestedMap("foo", ids);
            assertEquals(expectedEmptyMap, carpetReader.read());
        }
    }

    @Test
    void emptyNestedListIsSupported() throws IOException {

        record EmptyNestedCollection(String name, Map<String, List<String>> ids) {
        }

        var rec = new EmptyNestedCollection("foo", Map.of("key", List.of()));
        var writerTest = new ParquetWriterTest<>(EmptyNestedCollection.class);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void nullNestedListIsSupported() throws IOException {

        record EmptyNestedCollection(String name, Map<String, List<String>> ids) {
        }

        Map<String, List<String>> nullValue = new HashMap<>();
        nullValue.put("key", null);
        var rec = new EmptyNestedCollection("foo", nullValue);
        var writerTest = new ParquetWriterTest<>(EmptyNestedCollection.class);
        writerTest.write(rec);

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    // Map.of doesn't support null values
    private <T, R> Map<T, R> mapOf(T key1, R value1, T key2, R value2) {
        HashMap<T, R> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private Object unUtf8Map(Object obj) {
        if (obj instanceof Map<?, ?> map) {
            Map<Object, Object> res = new HashMap<>();
            for (var e : map.entrySet()) {
                Object key = e.getKey() instanceof Utf8 u ? u.toString() : e.getKey();
                Object value = e.getValue() instanceof Utf8 u ? u.toString() : e.getValue();
                if (value instanceof Map) {
                    value = unUtf8Map(value);
                }
                res.put(key, value);
            }
            return res;
        }
        return obj;
    }

    private Object unByteBufferMap(Object obj) throws IOException {
        if (obj instanceof Map<?, ?> map) {
            Map<Object, Object> res = new HashMap<>();
            for (var e : map.entrySet()) {
                Object key = e.getKey() instanceof Utf8 u ? u.toString() : e.getKey();
                Object value = e.getValue() instanceof ByteBuffer u ? new String(u.array(), "UTF-8") : e.getValue();
                if (value instanceof Map) {
                    value = unByteBufferMap(value);
                }
                res.put(key, value);
            }
            return res;
        }
        return obj;
    }

    private Object byteBufferMap(Object obj) throws IOException {
        if (obj instanceof Map<?, ?> map) {
            Map<Object, Object> res = new HashMap<>();
            for (var e : map.entrySet()) {
                Object key = e.getKey() instanceof Utf8 u ? u.toString() : e.getKey();
                Object value = e.getValue();
                if (value instanceof Map) {
                    value = unByteBufferMap(value);
                }
                res.put(key, value);
            }
            return res;
        }
        return obj;
    }

}
