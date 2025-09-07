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

import static com.jerolba.carpet.ColumnNamingStrategy.SNAKE_CASE;
import static java.nio.file.Files.createTempFile;
import static java.time.ZoneOffset.ofHours;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.FieldMatchingStrategy;
import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetGeography;
import com.jerolba.carpet.annotation.ParquetGeography.EdgeAlgorithm;
import com.jerolba.carpet.annotation.ParquetGeometry;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.annotation.ParquetString;
import com.jerolba.carpet.annotation.PrecisionScale;
import com.jerolba.carpet.annotation.Rounding;
import com.jerolba.carpet.io.FileSystemInputFile;
import com.jerolba.carpet.io.FileSystemOutputFile;

class CarpetWriterTest {

    enum Category {
        one, two, three;
    }

    @Nested
    class SimpleTypes {

        private final GeometryFactory geomFactory = new GeometryFactory();
        private final WKBWriter wkbWriter = new WKBWriter();

        @Test
        void intPrimitive() throws IOException {

            record IntPrimitive(int value) {
            }

            var rec1 = new IntPrimitive(1);
            var rec2 = new IntPrimitive(2);
            var writerTest = new ParquetWriterTest<>(IntPrimitive.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void longPrimitive() throws IOException {

            record LongPrimitive(long value) {
            }

            var rec1 = new LongPrimitive(191919191919L);
            var rec2 = new LongPrimitive(292929292929L);
            var writerTest = new ParquetWriterTest<>(LongPrimitive.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void doublePrimitive() throws IOException {

            record DoublePrimitive(double value) {
            }

            var rec1 = new DoublePrimitive(1.9);
            var rec2 = new DoublePrimitive(2.9);
            var writerTest = new ParquetWriterTest<>(DoublePrimitive.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void floatPrimitive() throws IOException {

            record FloatPrimitive(float value) {
            }

            var rec1 = new FloatPrimitive(1.9f);
            var rec2 = new FloatPrimitive(2.9f);
            var writerTest = new ParquetWriterTest<>(FloatPrimitive.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void shortPrimitive() throws IOException {

            record ShortPrimitive(short value) {
            }

            var rec1 = new ShortPrimitive((short) 1);
            var rec2 = new ShortPrimitive((short) 2);
            var writerTest = new ParquetWriterTest<>(ShortPrimitive.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, ((Number) avroReader.read().get("value")).shortValue());
                assertEquals(rec2.value, ((Number) avroReader.read().get("value")).shortValue());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void bytePrimitive() throws IOException {

            record BytePrimitive(byte value) {
            }

            var rec1 = new BytePrimitive((byte) 1);
            var rec2 = new BytePrimitive((byte) 2);
            var writerTest = new ParquetWriterTest<>(BytePrimitive.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, ((Number) avroReader.read().get("value")).byteValue());
                assertEquals(rec2.value, ((Number) avroReader.read().get("value")).byteValue());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void booleanPrimitive() throws IOException {

            record BooleanPrimitive(boolean value) {
            }

            var rec1 = new BooleanPrimitive(true);
            var rec2 = new BooleanPrimitive(false);
            var writerTest = new ParquetWriterTest<>(BooleanPrimitive.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void integerObject() throws IOException {

            record IntegerObject(Integer value) {
            }

            var rec1 = new IntegerObject(1);
            var rec2 = new IntegerObject(2);
            var writerTest = new ParquetWriterTest<>(IntegerObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void longObject() throws IOException {

            record LongObject(Long value) {
            }

            var rec1 = new LongObject(191919191919L);
            var rec2 = new LongObject(292929292929L);
            var writerTest = new ParquetWriterTest<>(LongObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void doubleObject() throws IOException {

            record DoubleObject(Double value) {
            }

            var rec1 = new DoubleObject(1.9);
            var rec2 = new DoubleObject(2.9);
            var writerTest = new ParquetWriterTest<>(DoubleObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void floatObject() throws IOException {

            record FloatObject(Float value) {
            }

            var rec1 = new FloatObject(1.9f);
            var rec2 = new FloatObject(2.9f);
            var writerTest = new ParquetWriterTest<>(FloatObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void shortObject() throws IOException {

            record ShortObject(Short value) {
            }

            var rec1 = new ShortObject((short) 1);
            var rec2 = new ShortObject((short) 2);
            var writerTest = new ParquetWriterTest<>(ShortObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, ((Number) avroReader.read().get("value")).shortValue());
                assertEquals(rec2.value, ((Number) avroReader.read().get("value")).shortValue());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void byteObject() throws IOException {

            record ByteObject(Byte value) {
            }

            var rec1 = new ByteObject((byte) 1);
            var rec2 = new ByteObject((byte) 2);
            var writerTest = new ParquetWriterTest<>(ByteObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, ((Number) avroReader.read().get("value")).byteValue());
                assertEquals(rec2.value, ((Number) avroReader.read().get("value")).byteValue());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void booleanObject() throws IOException {

            record BooleanObject(Boolean value) {
            }

            var rec1 = new BooleanObject(true);
            var rec2 = new BooleanObject(false);
            var writerTest = new ParquetWriterTest<>(BooleanObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertEquals(rec2.value, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void stringObject() throws IOException {

            record StringObject(String value) {
            }

            var rec1 = new StringObject("Madrid");
            var rec2 = new StringObject("Zaragoza");
            var writerTest = new ParquetWriterTest<>(StringObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value").toString());
                assertEquals(rec2.value, avroReader.read().get("value").toString());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void stringBinaryObject() throws IOException {

            record StringObject(@ParquetString Binary value) {
            }

            var rec1 = new StringObject(Binary.fromString("Madrid"));
            var rec2 = new StringObject(Binary.fromString("Zaragoza"));
            var writerTest = new ParquetWriterTest<>(StringObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value.toStringUsingUTF8(), avroReader.read().get("value").toString());
                assertEquals(rec2.value.toStringUsingUTF8(), avroReader.read().get("value").toString());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void jsonAsStringObject() throws IOException {

            record JsonAsStringObject(@ParquetJson String value) {
            }

            var rec1 = new JsonAsStringObject("{\"city\": \"Madrid\"}");
            var rec2 = new JsonAsStringObject("{\"city\": \"Zaragoza\"}");
            var writerTest = new ParquetWriterTest<>(JsonAsStringObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                ByteBuffer asByteBuffer1 = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec1.value, new String(asByteBuffer1.array()));
                ByteBuffer asByteBuffer2 = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec2.value, new String(asByteBuffer2.array()));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void jsonAsBinaryObject() throws IOException {

            record JsonAsBinaryObject(@ParquetJson Binary value) {
            }

            var rec1 = new JsonAsBinaryObject(Binary.fromString("{\"city\": \"Madrid\"}"));
            var rec2 = new JsonAsBinaryObject(Binary.fromString("{\"city\": \"Zaragoza\"}"));
            var writerTest = new ParquetWriterTest<>(JsonAsBinaryObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                ByteBuffer asByteBuffer1 = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec1.value, Binary.fromReusedByteBuffer(asByteBuffer1));
                ByteBuffer asByteBuffer2 = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec2.value, Binary.fromReusedByteBuffer(asByteBuffer2));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void bsonAsBinaryObject() throws IOException {

            record BsonAsBinaryObject(@ParquetBson Binary value) {
            }

            byte[] bson = new byte[] {
                    0x16, 0x00, 0x00, 0x00, // Total lenght (22 bytes) in little-endian
                    0x02, // Data type: String (0x02)
                    0x63, 0x69, 0x74, 0x79, 0x00, // "city" + null terminator
                    0x07, 0x00, 0x00, 0x00, // string lenght (7 bytes) in little-endian
                    0x4D, 0x61, 0x64, 0x72, 0x69, 0x64, 0x00, // "Madrid" + null terminator
                    0x00 // document terminator
            };
            var rec = new BsonAsBinaryObject(Binary.fromConstantByteArray(bson));
            var writerTest = new ParquetWriterTest<>(BsonAsBinaryObject.class);
            writerTest.write(rec);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                ByteBuffer asByteBuffer = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec.value, Binary.fromReusedByteBuffer(asByteBuffer));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec, carpetReader.read());
            }
        }

        @Test
        void geometryAsBinaryObject() throws IOException {

            record GeometryAsBinaryObject(@ParquetGeometry Binary value) {
            }

            Binary point = Binary
                    .fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0))));

            var rec = new GeometryAsBinaryObject(point);
            var writerTest = new ParquetWriterTest<>(GeometryAsBinaryObject.class);
            writerTest.write(rec);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                ByteBuffer asByteBuffer = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec.value, Binary.fromReusedByteBuffer(asByteBuffer));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec, carpetReader.read());
            }
        }

        @Test
        void geometryWithCrsAsBinaryObject() throws IOException {

            record GeometryAsBinaryObject(@ParquetGeometry("OGC:CRS84") Binary value) {
            }

            Binary point = Binary
                    .fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0))));

            var rec = new GeometryAsBinaryObject(point);
            var writerTest = new ParquetWriterTest<>(GeometryAsBinaryObject.class);
            writerTest.write(rec);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                ByteBuffer asByteBuffer = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec.value, Binary.fromReusedByteBuffer(asByteBuffer));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec, carpetReader.read());
            }
        }

        @Test
        void geographyAsBinaryObject() throws IOException {

            record GeographyAsBinaryObject(@ParquetGeography Binary value) {
            }

            Binary point = Binary
                    .fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0))));

            var rec = new GeographyAsBinaryObject(point);
            var writerTest = new ParquetWriterTest<>(GeographyAsBinaryObject.class);
            writerTest.write(rec);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                ByteBuffer asByteBuffer = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec.value, Binary.fromReusedByteBuffer(asByteBuffer));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec, carpetReader.read());
            }
        }

        @Test
        void geographyWithCrsAsBinaryObject() throws IOException {

            record GeographyAsBinaryObject(
                    @ParquetGeography(crs = "OGC:CRS84", edgeAlgorithm = EdgeAlgorithm.VINCENTY) Binary value) {
            }

            Binary point = Binary
                    .fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0))));

            var rec = new GeographyAsBinaryObject(point);
            var writerTest = new ParquetWriterTest<>(GeographyAsBinaryObject.class);
            writerTest.write(rec);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                ByteBuffer asByteBuffer = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec.value, Binary.fromReusedByteBuffer(asByteBuffer));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec, carpetReader.read());
            }
        }

        @Test
        void justBinaryObject() throws IOException {

            record JustBinaryObject(Binary value) {
            }

            byte[] byteArray = new byte[] { 0x01, 0x02, 0x03, 0x04 };
            var rec = new JustBinaryObject(Binary.fromConstantByteArray(byteArray));
            var writerTest = new ParquetWriterTest<>(JustBinaryObject.class);
            writerTest.write(rec);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                ByteBuffer asByteBuffer = (ByteBuffer) avroReader.read().get("value");
                assertEquals(rec.value, Binary.fromReusedByteBuffer(asByteBuffer));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec, carpetReader.read());
            }
        }

        @Test
        void enumObject() throws IOException {

            record EnumObject(Category value) {
            }

            var rec1 = new EnumObject(Category.one);
            var rec2 = new EnumObject(Category.two);
            var writerTest = new ParquetWriterTest<>(EnumObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value.name(), avroReader.read().get("value").toString());
                assertEquals(rec2.value.name(), avroReader.read().get("value").toString());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void enumAsStringObject() throws IOException {

            record EnumObject(@ParquetString Category value) {
            }

            var rec1 = new EnumObject(Category.one);
            var rec2 = new EnumObject(Category.two);
            var writerTest = new ParquetWriterTest<>(EnumObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value.name(), avroReader.read().get("value").toString());
                assertEquals(rec2.value.name(), avroReader.read().get("value").toString());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }

            record EnumStringObject(String value) {
            }

            var carpetReaderString = writerTest.getCarpetReader(EnumStringObject.class);
            assertEquals(new EnumStringObject("one"), carpetReaderString.read());
            assertEquals(new EnumStringObject("two"), carpetReaderString.read());
        }

        @Test
        void stringAsEnumObject() throws IOException {

            record EnumAsStringObject(@ParquetEnum String value) {
            }

            var rec1 = new EnumAsStringObject("one");
            var rec2 = new EnumAsStringObject("two");
            var writerTest = new ParquetWriterTest<>(EnumAsStringObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value").toString());
                assertEquals(rec2.value, avroReader.read().get("value").toString());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }

            record EnumObject(Category value) {
            }

            try (var carpetReaderString = writerTest.getCarpetReader(EnumObject.class)) {
                assertEquals(new EnumObject(Category.one), carpetReaderString.read());
                assertEquals(new EnumObject(Category.two), carpetReaderString.read());
            }
        }

        @Test
        void binaryAsEnumObject() throws IOException {

            record EnumAsBinaryObject(@ParquetEnum Binary value) {
            }

            var rec1 = new EnumAsBinaryObject(Binary.fromString("one"));
            var rec2 = new EnumAsBinaryObject(Binary.fromString("two"));
            var writerTest = new ParquetWriterTest<>(EnumAsBinaryObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value.toStringUsingUTF8(), avroReader.read().get("value").toString());
                assertEquals(rec2.value.toStringUsingUTF8(), avroReader.read().get("value").toString());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }

            record EnumObject(Category value) {
            }

            try (var carpetReaderString = writerTest.getCarpetReader(EnumObject.class)) {
                assertEquals(new EnumObject(Category.one), carpetReaderString.read());
                assertEquals(new EnumObject(Category.two), carpetReaderString.read());
            }
        }

        @Test
        void uuidObject() throws IOException {

            record UuidObject(UUID value) {
            }

            var rec1 = new UuidObject(UUID.randomUUID());
            var rec2 = new UuidObject(UUID.randomUUID());
            var writerTest = new ParquetWriterTest<>(UuidObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value.toString(), avroReader.read().get("value").toString());
                assertEquals(rec2.value.toString(), avroReader.read().get("value").toString());
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Nested
        class BigDecimalField {

            record BigDecimalObject(BigDecimal value) {
            }

            @Test
            void highPrecision() throws IOException {
                var bigDec1 = new BigDecimal("12345678901234.56789");
                var bigDec2 = new BigDecimal("98765432109876.54321");
                var rec1 = new BigDecimalObject(bigDec1);
                var rec2 = new BigDecimalObject(bigDec2);
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class)
                        .withDecimalConfig(20, 5);
                writerTest.write(rec1, rec2);

                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new DecimalConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(bigDec1, avroReader.read().get("value"));
                    assertEquals(bigDec2, avroReader.read().get("value"));
                }
                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void mediumPrecision() throws IOException {
                var bigDec1 = new BigDecimal("1234567890123.456");
                var bigDec2 = new BigDecimal("9876543210987.654");
                var rec1 = new BigDecimalObject(bigDec1);
                var rec2 = new BigDecimalObject(bigDec2);
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class)
                        .withDecimalConfig(18, 3);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void lowPrecision() throws IOException {
                var bigDec1 = new BigDecimal("12345.6789");
                var bigDec2 = new BigDecimal("98765.4321");
                var rec1 = new BigDecimalObject(bigDec1);
                var rec2 = new BigDecimalObject(bigDec2);
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class)
                        .withDecimalConfig(9, 4);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void rescaling() throws IOException {
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class)
                        .withDecimalConfig(20, 5);
                writerTest.write(
                        new BigDecimalObject(new BigDecimal("12345678901234.5")),
                        new BigDecimalObject(new BigDecimal("98765432109876.5")));

                BigDecimal scaled1 = new BigDecimal("12345678901234.50000");
                BigDecimal scaled2 = new BigDecimal("98765432109876.50000");
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new DecimalConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(scaled1, avroReader.read().get("value"));
                    assertEquals(scaled2, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(scaled1, carpetReader.read().value());
                    assertEquals(scaled2, carpetReader.read().value());
                }
            }

            @Test
            void invalidRescaling() {
                var rec1 = new BigDecimalObject(new BigDecimal("12345678901234.5678"));
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class)
                        .withDecimalConfig(20, 2);
                assertThrowsExactly(RecordTypeConversionException.class,
                        () -> writerTest.write(rec1));
            }

            @Test
            void invalidPrecision() {
                var rec1 = new BigDecimalObject(new BigDecimal("12345678901234.5678"));
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class)
                        .withDecimalConfig(10, 4);
                assertThrowsExactly(RecordTypeConversionException.class,
                        () -> writerTest.write(rec1));
            }

            @Test
            void allowScaleAdjustament() throws IOException {
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class)
                        .withBigDecimalScaleAdjustment(RoundingMode.HALF_UP)
                        .withDecimalConfig(20, 2);
                writerTest.write(
                        new BigDecimalObject(new BigDecimal("12345678901234.5678")),
                        new BigDecimalObject(new BigDecimal("12345678901234.1234")));

                BigDecimal scaled1 = new BigDecimal("12345678901234.57");
                BigDecimal scaled2 = new BigDecimal("12345678901234.12");
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new DecimalConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(scaled1, avroReader.read().get("value"));
                    assertEquals(scaled2, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(scaled1, carpetReader.read().value());
                    assertEquals(scaled2, carpetReader.read().value());
                }
            }
        }

        @Nested
        class BigDecimalFieldAnnotated {

            @Test
            void highPrecision() throws IOException {
                record BigDecimalObject(@PrecisionScale(precision = 20, scale = 5) BigDecimal value) {
                }

                var bigDec1 = new BigDecimal("12345678901234.56789");
                var bigDec2 = new BigDecimal("98765432109876.54321");
                var rec1 = new BigDecimalObject(bigDec1);
                var rec2 = new BigDecimalObject(bigDec2);
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class);
                writerTest.write(rec1, rec2);

                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new DecimalConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(bigDec1, avroReader.read().get("value"));
                    assertEquals(bigDec2, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void mediumPrecision() throws IOException {
                record BigDecimalObject(@PrecisionScale(precision = 18, scale = 3) BigDecimal value) {
                }

                var bigDec1 = new BigDecimal("1234567890123.456");
                var bigDec2 = new BigDecimal("9876543210987.654");
                var rec1 = new BigDecimalObject(bigDec1);
                var rec2 = new BigDecimalObject(bigDec2);
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void lowPrecision() throws IOException {
                record BigDecimalObject(@PrecisionScale(precision = 9, scale = 4) BigDecimal value) {
                }

                var bigDec1 = new BigDecimal("12345.6789");
                var bigDec2 = new BigDecimal("98765.4321");
                var rec1 = new BigDecimalObject(bigDec1);
                var rec2 = new BigDecimalObject(bigDec2);
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void rescaling() throws IOException {
                record BigDecimalObject(@PrecisionScale(precision = 20, scale = 5) BigDecimal value) {
                }

                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class);
                writerTest.write(
                        new BigDecimalObject(new BigDecimal("12345678901234.5")),
                        new BigDecimalObject(new BigDecimal("98765432109876.5")));

                BigDecimal scaled1 = new BigDecimal("12345678901234.50000");
                BigDecimal scaled2 = new BigDecimal("98765432109876.50000");
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new DecimalConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(scaled1, avroReader.read().get("value"));
                    assertEquals(scaled2, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(scaled1, carpetReader.read().value());
                    assertEquals(scaled2, carpetReader.read().value());
                }
            }

            @Test
            void invalidRescaling() {
                record BigDecimalObject(@PrecisionScale(precision = 20, scale = 2) BigDecimal value) {
                }
                var rec1 = new BigDecimalObject(new BigDecimal("12345678901234.5678"));
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class);
                assertThrowsExactly(RecordTypeConversionException.class,
                        () -> writerTest.write(rec1));
            }

            @Test
            void invalidPrecision() {
                record BigDecimalObject(@PrecisionScale(precision = 10, scale = 4) BigDecimal value) {
                }
                var rec1 = new BigDecimalObject(new BigDecimal("12345678901234.5678"));
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class);
                assertThrowsExactly(RecordTypeConversionException.class,
                        () -> writerTest.write(rec1));
            }

            @Test
            void allowScaleAdjustament() throws IOException {
                record BigDecimalObject(
                        @PrecisionScale(precision = 20, scale = 2) @Rounding(RoundingMode.HALF_UP) BigDecimal value) {
                }
                var writerTest = new ParquetWriterTest<>(BigDecimalObject.class);
                writerTest.write(
                        new BigDecimalObject(new BigDecimal("12345678901234.5678")),
                        new BigDecimalObject(new BigDecimal("12345678901234.1234")));

                BigDecimal scaled1 = new BigDecimal("12345678901234.57");
                BigDecimal scaled2 = new BigDecimal("12345678901234.12");
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new DecimalConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(scaled1, avroReader.read().get("value"));
                    assertEquals(scaled2, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(scaled1, carpetReader.read().value());
                    assertEquals(scaled2, carpetReader.read().value());
                }
            }

        }

        @Test
        void integerNullObject() throws IOException {

            record IntegerNullObject(Integer value) {
            }

            var rec1 = new IntegerNullObject(1);
            var rec2 = new IntegerNullObject(null);
            var writerTest = new ParquetWriterTest<>(IntegerNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void longNullObject() throws IOException {

            record LongNullObject(Long value) {
            }

            var rec1 = new LongNullObject(191919191919L);
            var rec2 = new LongNullObject(null);
            var writerTest = new ParquetWriterTest<>(LongNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void doubleNullObject() throws IOException {

            record DoubleNullObject(Double value) {
            }

            var rec1 = new DoubleNullObject(1.9);
            var rec2 = new DoubleNullObject(null);
            var writerTest = new ParquetWriterTest<>(DoubleNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void floatNullObject() throws IOException {

            record FloatNullObject(Float value) {
            }

            var rec1 = new FloatNullObject(1.9f);
            var rec2 = new FloatNullObject(null);
            var writerTest = new ParquetWriterTest<>(FloatNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void shortNullObject() throws IOException {

            record ShortNullObject(Short value) {
            }

            var rec1 = new ShortNullObject((short) 1);
            var rec2 = new ShortNullObject(null);
            var writerTest = new ParquetWriterTest<>(ShortNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, ((Number) avroReader.read().get("value")).shortValue());
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void byteNullObject() throws IOException {

            record ByteNullObject(Byte value) {
            }

            var rec1 = new ByteNullObject((byte) 1);
            var rec2 = new ByteNullObject(null);
            var writerTest = new ParquetWriterTest<>(ByteNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, ((Number) avroReader.read().get("value")).byteValue());
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void booleanNullObject() throws IOException {

            record BooleanNullObject(Boolean value) {
            }

            var rec1 = new BooleanNullObject(true);
            var rec2 = new BooleanNullObject(null);
            var writerTest = new ParquetWriterTest<>(BooleanNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value"));
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void stringNullObject() throws IOException {

            record StringNullObject(String value) {
            }

            var rec1 = new StringNullObject("Madrid");
            var rec2 = new StringNullObject(null);
            var writerTest = new ParquetWriterTest<>(StringNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value, avroReader.read().get("value").toString());
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void enumNullObject() throws IOException {

            record EnumNullObject(Category value) {
            }

            var rec1 = new EnumNullObject(Category.one);
            var rec2 = new EnumNullObject(null);
            var writerTest = new ParquetWriterTest<>(EnumNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value.name(), avroReader.read().get("value").toString());
                assertNull(avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void uuidNullObject() throws IOException {

            record UuidNullObject(UUID value) {
            }

            var rec1 = new UuidNullObject(UUID.randomUUID());
            var rec2 = new UuidNullObject(null);
            var writerTest = new ParquetWriterTest<>(UuidNullObject.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.value.toString(), avroReader.read().get("value").toString());
                assertEquals(null, avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }
    }

    @Nested
    class DateTypes {

        @Test
        void localDate() throws IOException {

            record LocalDateRecord(LocalDate value) {
            }

            var rec1 = new LocalDateRecord(LocalDate.of(2022, 11, 21));
            var rec2 = new LocalDateRecord(LocalDate.of(1976, 1, 15));
            var writerTest = new ParquetWriterTest<>(LocalDateRecord.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals((int) rec1.value.toEpochDay(), avroReader.read().get("value"));
                assertEquals((int) rec2.value.toEpochDay(), avroReader.read().get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Nested
        class LocalTimeType {

            record LocalTimeRecord(LocalTime value) {
            }

            LocalTimeRecord rec1 = new LocalTimeRecord(LocalTime.of(9, 30, 21, 100200300));
            LocalTimeRecord rec2 = new LocalTimeRecord(LocalTime.of(23, 59, 59, 999999999));

            @Test
            void timeMillis() throws IOException {

                var writerTest = new ParquetWriterTest<>(LocalTimeRecord.class)
                        .withTimeUnit(TimeUnit.MILLIS);
                writerTest.write(rec1, rec2);

                try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                    assertEquals((int) (rec1.value.toNanoOfDay() / 1000000), avroReader.read().get("value"));
                    assertEquals((int) (rec2.value.toNanoOfDay() / 1000000), avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(LocalTime.of(9, 30, 21, 100000000), carpetReader.read().value());
                    assertEquals(LocalTime.of(23, 59, 59, 999000000), carpetReader.read().value());
                }
            }

            @Test
            void timeMicros() throws IOException {

                var writerTest = new ParquetWriterTest<>(LocalTimeRecord.class)
                        .withTimeUnit(TimeUnit.MICROS);
                writerTest.write(rec1, rec2);

                try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                    assertEquals(rec1.value.toNanoOfDay() / 1000, avroReader.read().get("value"));
                    assertEquals(rec2.value.toNanoOfDay() / 1000, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(LocalTime.of(9, 30, 21, 100200000), carpetReader.read().value());
                    assertEquals(LocalTime.of(23, 59, 59, 999999000), carpetReader.read().value());
                }
            }

            @Test
            void timeNanos() throws IOException {

                var writerTest = new ParquetWriterTest<>(LocalTimeRecord.class)
                        .withTimeUnit(TimeUnit.NANOS);
                writerTest.write(rec1, rec2);

                try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                    assertEquals(rec1.value.toNanoOfDay(), avroReader.read().get("value"));
                    assertEquals(rec2.value.toNanoOfDay(), avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }
        }

        @Nested
        class InstantType {

            record InstantRecord(Instant value) {
            }

            private final LocalDateTime local = LocalDateTime.of(2024, 5, 3, 15, 31, 11);
            private final Instant instant1 = Instant.ofEpochSecond(local.toEpochSecond(ofHours(2)), 987654321);
            private final Instant instant2 = Instant.ofEpochSecond(local.toEpochSecond(ofHours(-2)), 123456789);
            private final InstantRecord rec1 = new InstantRecord(instant1);
            private final InstantRecord rec2 = new InstantRecord(instant2);

            @Test
            void millis() throws IOException {
                var writerTest = new ParquetWriterTest<>(InstantRecord.class)
                        .withTimeUnit(TimeUnit.MILLIS);
                writerTest.write(rec1, rec2);

                Instant expected1 = Instant.ofEpochSecond(local.toEpochSecond(ofHours(2)), 987000000);
                Instant expected2 = Instant.ofEpochSecond(local.toEpochSecond(ofHours(-2)), 123000000);

                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(expected1, avroReader.read().get("value"));
                    assertEquals(expected2, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(expected1, carpetReader.read().value());
                    assertEquals(expected2, carpetReader.read().value());
                }
            }

            @Test
            void micros() throws IOException {
                var writerTest = new ParquetWriterTest<>(InstantRecord.class)
                        .withTimeUnit(TimeUnit.MICROS);
                writerTest.write(rec1, rec2);

                Instant expected1 = Instant.ofEpochSecond(local.toEpochSecond(ofHours(2)), 987654000);
                Instant expected2 = Instant.ofEpochSecond(local.toEpochSecond(ofHours(-2)), 123456000);

                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(expected1, avroReader.read().get("value"));
                    assertEquals(expected2, avroReader.read().get("value"));
                }
                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(expected1, carpetReader.read().value());
                    assertEquals(expected2, carpetReader.read().value());
                }
            }

            @Test
            void nanos() throws IOException {
                var writerTest = new ParquetWriterTest<>(InstantRecord.class)
                        .withTimeUnit(TimeUnit.NANOS);
                writerTest.write(rec1, rec2);

                Instant expected1 = Instant.ofEpochSecond(local.toEpochSecond(ofHours(2)), 987654321);
                Instant expected2 = Instant.ofEpochSecond(local.toEpochSecond(ofHours(-2)), 123456789);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(expected1, carpetReader.read().value());
                    assertEquals(expected2, carpetReader.read().value());
                }
            }
        }

        @Nested
        class LocalDateTimeType {

            record LocalDateTimeRecord(LocalDateTime value) {
            }

            private final LocalDateTime local1 = LocalDateTime.of(2024, 5, 3, 15, 31, 11, 987654321);
            private final LocalDateTime local2 = LocalDateTime.of(2024, 5, 3, 23, 59, 59, 123456789);
            private final LocalDateTimeRecord rec1 = new LocalDateTimeRecord(local1);
            private final LocalDateTimeRecord rec2 = new LocalDateTimeRecord(local2);

            @Test
            @Disabled("Waiting for 1.14.0 release")
            void millis() throws IOException {
                var writerTest = new ParquetWriterTest<>(LocalDateTimeRecord.class)
                        .withTimeUnit(TimeUnit.MILLIS);
                writerTest.write(rec1, rec2);

                LocalDateTime expected1 = LocalDateTime.of(2024, 5, 3, 15, 31, 11, 987000000);
                LocalDateTime expected2 = LocalDateTime.of(2024, 5, 3, 23, 59, 59, 123000000);

                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(expected1, avroReader.read().get("value"));
                    assertEquals(expected2, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(expected1, carpetReader.read().value());
                    assertEquals(expected2, carpetReader.read().value());
                }
            }

            @Test
            // Remove after 1.14.0 release
            void millisByLong() throws IOException {
                var writerTest = new ParquetWriterTest<>(LocalDateTimeRecord.class)
                        .withTimeUnit(TimeUnit.MILLIS);
                writerTest.write(rec1, rec2);

                LocalDateTime expected1 = LocalDateTime.of(2024, 5, 3, 15, 31, 11, 987000000);
                LocalDateTime expected2 = LocalDateTime.of(2024, 5, 3, 23, 59, 59, 123000000);

                try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                    assertEquals(expected1.toEpochSecond(ZoneOffset.UTC) * 1000 + 987,
                            avroReader.read().get("value"));
                    assertEquals(expected2.toEpochSecond(ZoneOffset.UTC) * 1000 + 123,
                            avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(expected1, carpetReader.read().value());
                    assertEquals(expected2, carpetReader.read().value());
                }
            }

            @Test
            @Disabled("Waiting for 1.14.0 release")
            void micros() throws IOException {
                var writerTest = new ParquetWriterTest<>(LocalDateTimeRecord.class)
                        .withTimeUnit(TimeUnit.MICROS);
                writerTest.write(rec1, rec2);

                LocalDateTime expected1 = LocalDateTime.of(2024, 5, 3, 15, 31, 11, 987654000);
                LocalDateTime expected2 = LocalDateTime.of(2024, 5, 3, 23, 59, 59, 123456000);

                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
                try (var avroReader = writerTest.getAvroGenericRecordReaderWithModel(genericDataModel)) {
                    assertEquals(expected1, avroReader.read().get("value"));
                    assertEquals(expected2, avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(expected1, carpetReader.read().value());
                    assertEquals(expected2, carpetReader.read().value());
                }
            }

            @Test
            // Remove after 1.14.0 release
            void microsByLong() throws IOException {
                var writerTest = new ParquetWriterTest<>(LocalDateTimeRecord.class)
                        .withTimeUnit(TimeUnit.MICROS);
                writerTest.write(rec1, rec2);

                LocalDateTime expected1 = LocalDateTime.of(2024, 5, 3, 15, 31, 11, 987654000);
                LocalDateTime expected2 = LocalDateTime.of(2024, 5, 3, 23, 59, 59, 123456000);

                try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                    assertEquals(expected1.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 987654,
                            avroReader.read().get("value"));
                    assertEquals(expected2.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 123456,
                            avroReader.read().get("value"));
                }

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(expected1, carpetReader.read().value());
                    assertEquals(expected2, carpetReader.read().value());
                }
            }

            @Test
            void nanos() throws IOException {
                var writerTest = new ParquetWriterTest<>(LocalDateTimeRecord.class)
                        .withTimeUnit(TimeUnit.NANOS);
                writerTest.write(rec1, rec2);

                try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                    assertEquals(local1.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 987654321,
                            avroReader.read().get("value"));
                    assertEquals(local2.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789,
                            avroReader.read().get("value"));
                }
                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(local1, carpetReader.read().value());
                    assertEquals(local2, carpetReader.read().value());
                }
            }
        }
    }

    @Nested
    class InListTypes {

        @Test
        void integerList() throws IOException {

            record IntegerList(List<Integer> values) {
            }

            var rec1 = new IntegerList(asList(1, null, 3));
            var rec2 = new IntegerList(null);
            var writerTest = new ParquetWriterTest<>(IntegerList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void longList() throws IOException {

            record LongList(List<Long> values) {
            }

            var rec1 = new LongList(asList(191919191919L, null, 28282L));
            var rec2 = new LongList(null);
            var writerTest = new ParquetWriterTest<>(LongList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void doubleList() throws IOException {

            record DoubleList(List<Double> values) {
            }

            var rec1 = new DoubleList(asList(1.9, null, 2.9));
            var rec2 = new DoubleList(null);
            var writerTest = new ParquetWriterTest<>(DoubleList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void floatList() throws IOException {

            record FloatList(List<Float> values) {
            }

            var rec1 = new FloatList(asList(1.9f, null, 2.9f));
            var rec2 = new FloatList(null);
            var writerTest = new ParquetWriterTest<>(FloatList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void shortList() throws IOException {

            record ShortList(List<Short> values) {
            }

            var rec1 = new ShortList(asList((short) 1, null, (short) 3));
            var rec2 = new ShortList(null);
            var writerTest = new ParquetWriterTest<>(ShortList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void byteList() throws IOException {

            record ByteList(List<Byte> values) {
            }

            var rec1 = new ByteList(asList((byte) 1, null, (byte) 3));
            var rec2 = new ByteList(null);
            var writerTest = new ParquetWriterTest<>(ByteList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void booleanList() throws IOException {

            record BooleanList(List<Boolean> values) {
            }

            var rec1 = new BooleanList(asList(true, null, false));
            var rec2 = new BooleanList(null);
            var writerTest = new ParquetWriterTest<>(BooleanList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void stringList() throws IOException {

            record StringList(List<String> values) {
            }

            var rec1 = new StringList(asList("Madrid", null, "Zaragoza"));
            var rec2 = new StringList(null);
            var writerTest = new ParquetWriterTest<>(StringList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void enumList() throws IOException {

            record EnumList(List<Category> values) {
            }

            var rec1 = new EnumList(asList(Category.one, null, Category.two));
            var rec2 = new EnumList(null);
            var writerTest = new ParquetWriterTest<>(EnumList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void uuidList() throws IOException {

            record UuidList(List<UUID> values) {
            }

            var rec1 = new UuidList(asList(UUID.randomUUID(), null, UUID.randomUUID()));
            var rec2 = new UuidList(null);
            var writerTest = new ParquetWriterTest<>(UuidList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void localDateList() throws IOException {

            record LocalDateList(List<LocalDate> values) {
            }

            var rec1 = new LocalDateList(asList(LocalDate.of(2024, 1, 30), null, LocalDate.of(2024, 2, 20)));
            var rec2 = new LocalDateList(null);
            var writerTest = new ParquetWriterTest<>(LocalDateList.class);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Nested
        class LocalTimeListWrite {

            record LocalTimeList(List<LocalTime> values) {
            }

            LocalTimeList rec1 = new LocalTimeList(
                    asList(LocalTime.of(8, 10, 23, 123456789), null, LocalTime.of(10, 0, 0, 0)));
            LocalTimeList rec2 = new LocalTimeList(null);

            @Test
            void localTimeMillisList() throws IOException {
                var writerTest = new ParquetWriterTest<>(LocalTimeList.class)
                        .withTimeUnit(TimeUnit.MILLIS);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    LocalTimeList read1 = carpetReader.read();
                    assertEquals(LocalTime.of(8, 10, 23, 123000000), read1.values.get(0));
                    assertNull(read1.values.get(1));
                    assertEquals(LocalTime.of(10, 0, 0, 0), read1.values.get(2));
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void localTimeMicrosList() throws IOException {
                var writerTest = new ParquetWriterTest<>(LocalTimeList.class)
                        .withTimeUnit(TimeUnit.MICROS);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    LocalTimeList read1 = carpetReader.read();
                    assertEquals(LocalTime.of(8, 10, 23, 123456000), read1.values.get(0));
                    assertNull(read1.values.get(1));
                    assertEquals(LocalTime.of(10, 0, 0, 0), read1.values.get(2));
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void localTimeNanosList() throws IOException {
                var writerTest = new ParquetWriterTest<>(LocalTimeList.class)
                        .withTimeUnit(TimeUnit.NANOS);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }
        }

        @Nested
        class TimeStampListWrite {

            @Test
            void localDateTimeList() throws IOException {

                record LocalDateTimeList(List<LocalDateTime> values) {
                }

                LocalDateTimeList rec1 = new LocalDateTimeList(
                        asList(LocalDateTime.of(2024, 5, 3, 8, 10, 23, 123456789), null,
                                LocalDateTime.of(1976, 1, 15, 10, 0, 0, 0)));
                LocalDateTimeList rec2 = new LocalDateTimeList(null);

                var writerTest = new ParquetWriterTest<>(LocalDateTimeList.class)
                        .withTimeUnit(TimeUnit.NANOS);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }

            @Test
            void intantList() throws IOException {

                record InstantList(List<Instant> values) {
                }

                InstantList rec1 = new InstantList(asList(
                        LocalDateTime.of(2024, 5, 3, 8, 10, 23, 123456789).toInstant(ZoneOffset.ofHours(3)),
                        null,
                        LocalDateTime.of(1976, 1, 15, 10, 0, 0, 0).toInstant(ZoneOffset.ofHours(2))));
                InstantList rec2 = new InstantList(null);

                var writerTest = new ParquetWriterTest<>(InstantList.class)
                        .withTimeUnit(TimeUnit.NANOS);
                writerTest.write(rec1, rec2);

                try (var carpetReader = writerTest.getCarpetReader()) {
                    assertEquals(rec1, carpetReader.read());
                    assertEquals(rec2, carpetReader.read());
                }
            }
        }

        @Test
        void bigDecimalList() throws IOException {

            record BigDecimalList(List<BigDecimal> values) {
            }

            var rec1 = new BigDecimalList(asList(new BigDecimal("1234567.123"), null, BigDecimal.TEN));
            var rec2 = new BigDecimalList(null);
            var writerTest = new ParquetWriterTest<>(BigDecimalList.class)
                    .withDecimalConfig(20, 3);
            writerTest.write(rec1, rec2);

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(
                        new BigDecimalList(
                                asList(new BigDecimal("1234567.123"), null, new BigDecimal("10.000"))),
                        carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }
    }

    @Test
    void emptyFile() throws IOException {

        record EmptyFile(String someValue) {

        }
        var writerTest = new ParquetWriterTest<>(EmptyFile.class);
        writerTest.write();

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            assertNull(avroReader.read());
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertNull(carpetReader.read());
        }
    }

    @Test
    void classWithMultipleFields() throws IOException {

        record ClassWithMultipleFields(String name, Category category,
                int p1, Integer f1, long p2, Long f2, double p3, Double f3, float p4, Float f4,
                byte p5, Byte f5, short p6, Short f6, boolean p7, Boolean f7) {
        }

        var rec = new ClassWithMultipleFields("Apple", Category.one,
                1, 2, 3L, 4L, 5.1, 6.2, 7.3f, 8.4f,
                (byte) 9, (byte) 10, (short) 11, (short) 12, true, false);

        var writerTest = new ParquetWriterTest<>(ClassWithMultipleFields.class);
        writerTest.write(rec);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord record = avroReader.read();
            assertEquals(rec.name, record.get("name").toString());
            assertEquals(rec.category.name(), record.get("category").toString());
            assertEquals(rec.p1, record.get("p1"));
            assertEquals(rec.f1, record.get("f1"));
            assertEquals(rec.p2, record.get("p2"));
            assertEquals(rec.f2, record.get("f2"));
            assertEquals(rec.p3, record.get("p3"));
            assertEquals(rec.f3, record.get("f3"));
            assertEquals(rec.p4, record.get("p4"));
            assertEquals(rec.f4, record.get("f4"));
            assertEquals(rec.p5, ((Number) record.get("p5")).byteValue());
            assertEquals(rec.f5, ((Number) record.get("f5")).byteValue());
            assertEquals(rec.p6, ((Number) record.get("p6")).shortValue());
            assertEquals(rec.f6, ((Number) record.get("f6")).shortValue());
            assertEquals(rec.p7, record.get("p7"));
            assertEquals(rec.f7, record.get("f7"));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Test
    void classWithMultipleNullFields() throws IOException {

        record classWithMultipleNullFields(String name, Category category,
                int p1, Integer f1, long p2, Long f2, double p3, Double f3, float p4, Float f4,
                byte p5, Byte f5, short p6, Short f6, boolean p7, Boolean f7) {
        }

        var rec = new classWithMultipleNullFields(null, null,
                101, null, 103L, null, 105.1, null, 107.3f, null,
                (byte) 109, null, (short) 1011, null, false, null);
        var writerTest = new ParquetWriterTest<>(classWithMultipleNullFields.class);
        writerTest.write(rec);

        try (var avroReader = writerTest.getAvroGenericRecordReader()) {
            GenericRecord record = avroReader.read();
            assertNull(record.get("name"));
            assertNull(record.get("category"));
            assertEquals(rec.p1, record.get("p1"));
            assertNull(record.get("f1"));
            assertEquals(rec.p2, record.get("p2"));
            assertNull(record.get("f2"));
            assertEquals(rec.p3, record.get("p3"));
            assertNull(record.get("f3"));
            assertEquals(rec.p4, record.get("p4"));
            assertNull(record.get("f4"));
            assertEquals(rec.p5, ((Number) record.get("p5")).byteValue());
            assertNull(record.get("f5"));
            assertEquals(rec.p6, ((Number) record.get("p6")).shortValue());
            assertNull(record.get("f6"));
            assertEquals(rec.p7, record.get("p7"));
            assertNull(record.get("f7"));
        }

        try (var carpetReader = writerTest.getCarpetReader()) {
            assertEquals(rec, carpetReader.read());
        }
    }

    @Nested
    class GenericFieldsAreNotSupported {

        @Test
        void classWithGenericField() {

            record ClassWithGenericField<T>(String name, T value) {
            }

            var rec1 = new ClassWithGenericField<>("Apple", 1);
            var rec2 = new ClassWithGenericField<>("Google", 2);
            var writerTest = new ParquetWriterTest<>(ClassWithGenericField.class);
            assertThrows(RecordTypeConversionException.class, () -> writerTest.write(rec1, rec2));
        }

    }

    @Nested
    class CompositedClasses {

        @Test
        void compositeValue() throws IOException {

            record Child(String id, int value) {
            }
            record CompositeMain(String name, Child child) {
            }

            CompositeMain rec1 = new CompositeMain("Madrid", new Child("Population", 100));
            CompositeMain rec2 = new CompositeMain("Santander", new Child("Population", 200));

            var writerTest = new ParquetWriterTest<>(CompositeMain.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                GenericRecord record = avroReader.read();
                assertEquals(rec1.name, record.get("name").toString());
                GenericRecord child = (GenericRecord) record.get("child");
                assertEquals(rec1.child.id, child.get("id").toString());
                assertEquals(rec1.child.value, child.get("value"));
                record = avroReader.read();
                assertEquals(rec2.name, record.get("name").toString());
                child = (GenericRecord) record.get("child");
                assertEquals(rec2.child.id, child.get("id").toString());
                assertEquals(rec2.child.value, child.get("value"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void compositeNullValue() throws IOException {

            record Child(String id, int value) {
            }
            record CompositeNullMain(String name, Child child) {
            }

            CompositeNullMain rec1 = new CompositeNullMain("Madrid", new Child("Age", 100));
            CompositeNullMain rec2 = new CompositeNullMain("Santander", null);

            var writerTest = new ParquetWriterTest<>(CompositeNullMain.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                GenericRecord record = avroReader.read();
                assertEquals(rec1.name, record.get("name").toString());
                GenericRecord child = (GenericRecord) record.get("child");
                assertEquals(rec1.child.id, child.get("id").toString());
                assertEquals(rec1.child.value, child.get("value"));
                record = avroReader.read();
                assertEquals(rec2.name, record.get("name").toString());
                assertNull(record.get("child"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
            }
        }

        @Test
        void compositeGenericNotSupported() {

            interface Some {
                String id();
            }

            record Child(String id, int value) implements Some {
            }

            // Just for testing, Record types are final and can not be extended
            record CompositeGeneric<T extends Some>(String name, T child) {
            }

            CompositeGeneric<Some> rec1 = new CompositeGeneric<>("Madrid", new Child("Age", 100));
            CompositeGeneric<Some> rec2 = new CompositeGeneric<>("Santander", null);

            var writerTest = new ParquetWriterTest<>(CompositeGeneric.class);

            assertThrows(RecordTypeConversionException.class, () -> writerTest.write(rec1, rec2));
        }

        public record RecursiveLoop(String id, RercursiveMain recursive) {
        }

        public record RecursiveChild(String id, RecursiveLoop child) {
        }

        public record RercursiveMain(String name, RecursiveChild child) {
        }

        @Test
        void recursiveCompositeNotSupported() {

            RercursiveMain tail = new RercursiveMain("Tail", null);
            RecursiveLoop child2 = new RecursiveLoop("Level 3", tail);
            RecursiveChild child = new RecursiveChild("Level 2", child2);
            RercursiveMain main = new RercursiveMain("Level 1", child);

            var writerTest = new ParquetWriterTest<>(RercursiveMain.class);
            assertThrows(RecordTypeConversionException.class, () -> writerTest.write(main, tail));
        }

        @Test
        void multipleLevelComposition() throws IOException {

            record ThirdLevel(String area, long value) {
            }
            record SecondLevel(String name, ThirdLevel subChild) {
            }
            record MultipleLevelComposition(String id, SecondLevel child) {
            }

            MultipleLevelComposition rec1 = new MultipleLevelComposition("First",
                    new SecondLevel("Second", new ThirdLevel("Third", 20102032L)));
            MultipleLevelComposition rec2 = new MultipleLevelComposition("One",
                    new SecondLevel("Two", null));
            MultipleLevelComposition rec3 = new MultipleLevelComposition("Uno", null);

            var writerTest = new ParquetWriterTest<>(MultipleLevelComposition.class);
            writerTest.write(rec1, rec2, rec3);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                GenericRecord record1 = avroReader.read();
                assertEquals(rec1.id, record1.get("id").toString());
                GenericRecord child1 = (GenericRecord) record1.get("child");
                assertEquals(rec1.child.name, child1.get("name").toString());
                GenericRecord subChild1 = (GenericRecord) child1.get("subChild");
                assertEquals(rec1.child.subChild.area, subChild1.get("area").toString());
                assertEquals(rec1.child.subChild.value, subChild1.get("value"));

                GenericRecord record2 = avroReader.read();
                assertEquals(rec2.id, record2.get("id").toString());
                GenericRecord child2 = (GenericRecord) record2.get("child");
                assertEquals(rec2.child.name, child2.get("name").toString());
                assertNull(child2.get("subChild"));

                GenericRecord record3 = avroReader.read();
                assertEquals(rec3.id, record3.get("id").toString());
                assertNull(record3.get("child"));
            }

            try (var carpetReader = writerTest.getCarpetReader()) {
                assertEquals(rec1, carpetReader.read());
                assertEquals(rec2, carpetReader.read());
                assertEquals(rec3, carpetReader.read());
            }
        }

    }

    @Nested
    class RecordVisibility {

        public record PublicDeclaredRecord(String id, int value) {
        }

        @Test
        void publicDeclaredRecord() throws IOException {
            var rec1 = new PublicDeclaredRecord("Madrid", 10);
            var rec2 = new PublicDeclaredRecord("Barcelona", 20);
            var writerTest = new ParquetWriterTest<>(PublicDeclaredRecord.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.id, avroReader.read().get("id").toString());
                assertEquals(rec2.id, avroReader.read().get("id").toString());
            }
        }

        private record PrivateDeclaredRecord(String id, int value) {
        }

        @Test
        void privateDeclaredRecord() throws IOException {
            var rec1 = new PrivateDeclaredRecord("Madrid", 10);
            var rec2 = new PrivateDeclaredRecord("Barcelona", 20);
            var writerTest = new ParquetWriterTest<>(PrivateDeclaredRecord.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.id, avroReader.read().get("id").toString());
                assertEquals(rec2.id, avroReader.read().get("id").toString());
            }
        }

        @Test
        void methodDeclaredRecord() throws IOException {
            record MethodDeclaredRecord(String id, int value) {
            }

            var rec1 = new MethodDeclaredRecord("Madrid", 10);
            var rec2 = new MethodDeclaredRecord("Barcelona", 20);
            var writerTest = new ParquetWriterTest<>(MethodDeclaredRecord.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.id, avroReader.read().get("id").toString());
                assertEquals(rec2.id, avroReader.read().get("id").toString());
            }
        }
    }

    @Nested
    class ColumnNameConversion {

        @Nested
        class Aliased {

            @Test
            void fieldAliased() throws IOException {
                record SomeClass(@Alias("foo") long someId, @Alias("bar") int operation, boolean active) {
                }

                var writerTest = new ParquetWriterTest<>(SomeClass.class);
                writerTest.write(new SomeClass(1L, 2, true));

                String expected = """
                        message SomeClass {
                          required int64 foo;
                          required int32 bar;
                          required boolean active;
                        }
                        """;
                assertEquals(expected, writerTest.getSchema().toString());

                record ReadClass(long foo, int bar, boolean active) {
                }
                ReadClass value = writerTest.getCarpetReader(ReadClass.class).read();
                assertEquals(new ReadClass(1L, 2, true), value);
            }
        }

        @Nested
        class ToSnakeCase {

            @Test
            void fromCamelCase() throws IOException {
                record SomeClass(long someId, int operationCode, int with3) {
                }

                var writerTest = new ParquetWriterTest<>(SomeClass.class)
                        .withNameStrategy(SNAKE_CASE);
                writerTest.write(new SomeClass(1L, 2, 3));

                String expected = """
                        message SomeClass {
                          required int64 some_id;
                          required int32 operation_code;
                          required int32 with3;
                        }
                        """;
                assertEquals(expected, writerTest.getSchema().toString());

                record Some_Class(long some_id, int operation_code, int with3) {
                }
                Some_Class value = writerTest.getCarpetReader(Some_Class.class).read();
                assertEquals(new Some_Class(1L, 2, 3), value);
            }

            @Test
            void readAgainWithSnakeCaseStrategy() throws IOException {
                record SomeClass(long someId, int operationCode, int with3) {
                }

                var writerTest = new ParquetWriterTest<>(SomeClass.class)
                        .withNameStrategy(SNAKE_CASE);
                writerTest.write(new SomeClass(1L, 2, 3));

                CarpetReader<SomeClass> reader = new CarpetReader<>(writerTest.getTestFile(), SomeClass.class)
                        .withFieldMatchingStrategy(FieldMatchingStrategy.SNAKE_CASE);
                SomeClass read = reader.stream().toList().get(0);
                assertEquals(new SomeClass(1L, 2, 3), read);
            }

            @Test
            void alreadySnakeCase() throws IOException {
                record SomeClass(long some_id, int operation_code, int with3) {
                }

                var writerTest = new ParquetWriterTest<>(SomeClass.class)
                        .withNameStrategy(SNAKE_CASE);
                writerTest.write(new SomeClass(1L, 2, 3));

                String expected = """
                        message SomeClass {
                          required int64 some_id;
                          required int32 operation_code;
                          required int32 with3;
                        }
                        """;
                assertEquals(expected, writerTest.getSchema().toString());

                assertEquals(new SomeClass(1L, 2, 3), writerTest.getCarpetReader().read());
            }

        }

    }

    @Test
    void writesMetadataInParquetFile() throws IOException {

        record SomeEntity(int value) {
        }

        var file = createTempFile("metadata", ".parquet").toFile();
        try (var writer = new CarpetWriter.Builder<>(new FileSystemOutputFile(file), SomeEntity.class)
                .withExtraMetaData("someKey", "someValue")
                .withExtraMetaData(Map.of("foo", "bar"))
                .build()) {
            writer.write(new SomeEntity(1));
        }

        ParquetFileReader reader = ParquetFileReader.open(new FileSystemInputFile(file));
        Map<String, String> metadata = reader.getFileMetaData().getKeyValueMetaData();
        assertEquals("someValue", metadata.get("someKey"));
        assertEquals("bar", metadata.get("foo"));
    }

    @Nested
    class VisibilityTest {

        public record PublicRecord(String id, int value) {
        }

        private record PrivateRecord(String id, int value) {
        }

        record PackagePrivateRecord(String id, int value) {
        }

        @Test
        void publicRecordIsWritable() throws IOException {
            var rec1 = new PublicRecord("Madrid", 10);
            var rec2 = new PublicRecord("Barcelona", 20);

            var writerTest = new ParquetWriterTest<>(PublicRecord.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.id, avroReader.read().get("id").toString());
                assertEquals(rec2.id, avroReader.read().get("id").toString());
            }
        }

        @Test
        void privateRecordIsWritable() throws IOException {
            var rec1 = new PrivateRecord("Madrid", 10);
            var rec2 = new PrivateRecord("Barcelona", 20);

            var writerTest = new ParquetWriterTest<>(PrivateRecord.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.id, avroReader.read().get("id").toString());
                assertEquals(rec2.id, avroReader.read().get("id").toString());
            }
        }

        @Test
        void packagePrivateRecordIsWritable() throws IOException {
            var rec1 = new PackagePrivateRecord("Madrid", 10);
            var rec2 = new PackagePrivateRecord("Barcelona", 20);

            var writerTest = new ParquetWriterTest<>(PackagePrivateRecord.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.id, avroReader.read().get("id").toString());
                assertEquals(rec2.id, avroReader.read().get("id").toString());
            }
        }

        @Test
        void innerRecordIsWritable() throws IOException {

            record InnerRecord(String id, int value) {
            }

            var rec1 = new InnerRecord("Madrid", 10);
            var rec2 = new InnerRecord("Barcelona", 20);

            var writerTest = new ParquetWriterTest<>(InnerRecord.class);
            writerTest.write(rec1, rec2);

            try (var avroReader = writerTest.getAvroGenericRecordReader()) {
                assertEquals(rec1.id, avroReader.read().get("id").toString());
                assertEquals(rec2.id, avroReader.read().get("id").toString());
            }
        }
    }

}
