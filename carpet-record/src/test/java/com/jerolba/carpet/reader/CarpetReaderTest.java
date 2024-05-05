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

import static com.jerolba.carpet.FieldMatchingStrategy.BEST_EFFORT;
import static com.jerolba.carpet.FieldMatchingStrategy.FIELD_NAME;
import static com.jerolba.carpet.FieldMatchingStrategy.SNAKE_CASE;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.AnnotatedLevels;
import com.jerolba.carpet.CarpetMissingColumnException;
import com.jerolba.carpet.CarpetParquetReader;
import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.ParquetReaderTest;
import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.ReadFlag;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.io.FileSystemInputFile;
import com.jerolba.carpet.io.FileSystemOutputFile;

class CarpetReaderTest {

    enum Category {
        one, two, three;
    }

    @Nested
    class SimpleTypes {

        @Test
        void intPrimitive() throws IOException {
            Schema schema = schemaType("IntPrimitive").requiredInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2);
                writer.write(record);
            });

            record IntPrimitive(int value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(IntPrimitive.class)) {
                assertEquals(new IntPrimitive(1), carpetReader.read());
                assertEquals(new IntPrimitive(2), carpetReader.read());
            }
        }

        @Test
        void longPrimitive() throws IOException {
            Schema schema = schemaType("LongPrimitive").requiredLong("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 191919191919L);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 292929292929L);
                writer.write(record);
            });

            record LongPrimitive(long value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(LongPrimitive.class)) {
                assertEquals(new LongPrimitive(191919191919L), carpetReader.read());
                assertEquals(new LongPrimitive(292929292929L), carpetReader.read());
            }
        }

        @Test
        void doublePrimitive() throws IOException {
            Schema schema = schemaType("DoublePrimitive").requiredDouble("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1.9);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2.9);
                writer.write(record);
            });

            record DoublePrimitive(double value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(DoublePrimitive.class)) {
                assertEquals(new DoublePrimitive(1.9), carpetReader.read());
                assertEquals(new DoublePrimitive(2.9), carpetReader.read());
            }
        }

        @Test
        void floatPrimitive() throws IOException {
            Schema schema = schemaType("FloatPrimitive").requiredFloat("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1.9f);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2.9f);
                writer.write(record);
            });

            record FloatPrimitive(float value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(FloatPrimitive.class)) {
                assertEquals(new FloatPrimitive(1.9f), carpetReader.read());
                assertEquals(new FloatPrimitive(2.9f), carpetReader.read());
            }
        }

        @Test
        void shortPrimitive() throws IOException {
            Schema schema = schemaType("ShortPrimitive").requiredInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2);
                writer.write(record);
            });

            record ShortPrimitive(short value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(ShortPrimitive.class)) {
                assertEquals(new ShortPrimitive((short) 1), carpetReader.read());
                assertEquals(new ShortPrimitive((short) 2), carpetReader.read());
            }
        }

        @Test
        void bytePrimitive() throws IOException {
            Schema schema = schemaType("BytePrimitive").requiredInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2);
                writer.write(record);
            });

            record BytePrimitive(byte value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(BytePrimitive.class)) {
                assertEquals(new BytePrimitive((byte) 1), carpetReader.read());
                assertEquals(new BytePrimitive((byte) 2), carpetReader.read());
            }
        }

        @Test
        void booleanPrimitive() throws IOException {
            Schema schema = schemaType("BooleanPrimitive").requiredBoolean("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", true);
                writer.write(record);
                record = new Record(schema);
                record.put("value", false);
                writer.write(record);
            });

            record BooleanPrimitive(boolean value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(BooleanPrimitive.class)) {
                assertEquals(new BooleanPrimitive(true), carpetReader.read());
                assertEquals(new BooleanPrimitive(false), carpetReader.read());
            }
        }

        @Test
        void integerObject() throws IOException {
            Schema schema = schemaType("IntegerObject").optionalInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2);
                writer.write(record);
            });

            record IntegerObject(Integer value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(IntegerObject.class)) {
                assertEquals(new IntegerObject(1), carpetReader.read());
                assertEquals(new IntegerObject(2), carpetReader.read());
            }
        }

        @Test
        void longObject() throws IOException {
            Schema schema = schemaType("LongObject").optionalLong("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 191919191919L);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 292929292929L);
                writer.write(record);
            });

            record LongObject(Long value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(LongObject.class)) {
                assertEquals(new LongObject(191919191919L), carpetReader.read());
                assertEquals(new LongObject(292929292929L), carpetReader.read());
            }
        }

        @Test
        void doubleObject() throws IOException {
            Schema schema = schemaType("DoubleObject").optionalDouble("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1.9);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2.9);
                writer.write(record);
            });

            record DoubleObject(Double value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(DoubleObject.class)) {
                assertEquals(new DoubleObject(1.9), carpetReader.read());
                assertEquals(new DoubleObject(2.9), carpetReader.read());
            }
        }

        @Test
        void floatObject() throws IOException {
            Schema schema = schemaType("FloatObject").optionalFloat("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1.9f);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2.9f);
                writer.write(record);
            });

            record FloatObject(Float value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(FloatObject.class)) {
                assertEquals(new FloatObject(1.9f), carpetReader.read());
                assertEquals(new FloatObject(2.9f), carpetReader.read());
            }
        }

        @Test
        void shortObject() throws IOException {
            Schema schema = schemaType("ShortObject").optionalInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2);
                writer.write(record);
            });

            record ShortObject(Short value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(ShortObject.class)) {
                assertEquals(new ShortObject((short) 1), carpetReader.read());
                assertEquals(new ShortObject((short) 2), carpetReader.read());
            }
        }

        @Test
        void byteObject() throws IOException {
            Schema schema = schemaType("ByteObject").optionalInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", 2);
                writer.write(record);
            });

            record ByteObject(Byte value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(ByteObject.class)) {
                assertEquals(new ByteObject((byte) 1), carpetReader.read());
                assertEquals(new ByteObject((byte) 2), carpetReader.read());
            }
        }

        @Test
        void booleanObject() throws IOException {
            Schema schema = schemaType("BooleanObject").optionalBoolean("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", true);
                writer.write(record);
                record = new Record(schema);
                record.put("value", false);
                writer.write(record);
            });

            record BooleanObject(Boolean value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(BooleanObject.class)) {
                assertEquals(new BooleanObject(true), carpetReader.read());
                assertEquals(new BooleanObject(false), carpetReader.read());
            }
        }

        @Test
        void stringObject() throws IOException {
            Schema schema = schemaType("StringObject").optionalString("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", "Madrid");
                writer.write(record);
                record = new Record(schema);
                record.put("value", "Zaragoza");
                writer.write(record);
            });

            record StringObject(String value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(StringObject.class)) {
                assertEquals(new StringObject("Madrid"), carpetReader.read());
                assertEquals(new StringObject("Zaragoza"), carpetReader.read());
            }
        }

        @Test
        void enumObject() throws IOException {
            Schema schema = schemaType("EnumObject").name("value").type().nullable()
                    .enumeration("Category").symbols("one", "two", "three").noDefault()
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", "one");
                writer.write(record);
                record = new Record(schema);
                record.put("value", "two");
                writer.write(record);
            });

            record EnumObject(Category value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(EnumObject.class)) {
                assertEquals(new EnumObject(Category.one), carpetReader.read());
                assertEquals(new EnumObject(Category.two), carpetReader.read());
            }
        }

        @Test
        void uuidObject() throws IOException {
            Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
            Schema schema = SchemaBuilder.builder().record("UUID").fields()
                    .name("value").type(uuid).noDefault()
                    .endRecord();

            File file = new File(ParquetReaderTest.getTestFilePath("uuid"));
            var uuid1 = UUID.randomUUID();
            var uuid2 = UUID.randomUUID();
            OutputFile output = new FileSystemOutputFile(file);
            try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                    .config(AvroWriteSupport.WRITE_PARQUET_UUID, "true")
                    .withSchema(schema)
                    .build()) {
                Record record = new Record(schema);
                record.put("value", uuid1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", uuid2);
                writer.write(record);
            }

            record UuidType(UUID value) {
            }

            InputFile inputFile = new FileSystemInputFile(file);
            try (var carpetReader = CarpetParquetReader.builder(inputFile, UuidType.class).build()) {
                assertEquals(new UuidType(uuid1), carpetReader.read());
                assertEquals(new UuidType(uuid2), carpetReader.read());
            }
        }

        @Nested
        class ReadBigDecimal {

            @Test
            void bigDecimal() throws IOException {
                Schema decimal = LogicalTypes.decimal(15, 2).addToSchema(Schema.create(Schema.Type.BYTES));
                Schema schema = SchemaBuilder.builder().record("BigDecimal").fields()
                        .name("value").type(decimal).noDefault()
                        .endRecord();

                var bigDec1 = new BigDecimal("101201020.10");
                var bigDec2 = new BigDecimal("1120102034234.10");

                var readerTest = new ParquetReaderTest(schema);
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new DecimalConversion());
                readerTest.writerWithModel(genericDataModel, writer -> {
                    Record record = new Record(schema);
                    record.put("value", bigDec1);
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", bigDec2);
                    writer.write(record);
                });

                record BigDecimalType(BigDecimal value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(BigDecimalType.class)) {
                    assertEquals(new BigDecimalType(bigDec1), carpetReader.read());
                    assertEquals(new BigDecimalType(bigDec2), carpetReader.read());
                }
            }

            @Test
            void fromFixedByteArray() throws IOException {
                Schema decimal = LogicalTypes.decimal(15, 2).addToSchema(Schema.createFixed(null, null, null, 7));
                Schema schema = SchemaBuilder.builder().record("BigDecimal").fields()
                        .name("value").type(decimal).noDefault()
                        .endRecord();

                var bigDec1 = new BigDecimal("101201020.10");
                var bigDec2 = new BigDecimal("1120102034234.10");

                var readerTest = new ParquetReaderTest(schema);
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new DecimalConversion());
                readerTest.writerWithModel(genericDataModel, writer -> {
                    Record record = new Record(schema);
                    record.put("value", bigDec1);
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", bigDec2);
                    writer.write(record);
                });

                record BigDecimalType(BigDecimal value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(BigDecimalType.class)) {
                    assertEquals(new BigDecimalType(bigDec1), carpetReader.read());
                    assertEquals(new BigDecimalType(bigDec2), carpetReader.read());
                }
            }

        }

        @Test
        void integerNullObject() throws IOException {
            Schema schema = schemaType("IntegerNullObject").optionalInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record IntegerNullObject(Integer value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(IntegerNullObject.class)) {
                assertEquals(new IntegerNullObject(1), carpetReader.read());
                assertEquals(new IntegerNullObject(null), carpetReader.read());
            }
        }

        @Test
        void longNullObject() throws IOException {
            Schema schema = schemaType("LongNullObject").optionalLong("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 191919191919L);
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record LongNullObject(Long value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(LongNullObject.class)) {
                assertEquals(new LongNullObject(191919191919L), carpetReader.read());
                assertEquals(new LongNullObject(null), carpetReader.read());
            }
        }

        @Test
        void doubleNullObject() throws IOException {
            Schema schema = schemaType("DoubleNullObject").optionalDouble("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1.9);
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record DoubleNullObject(Double value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(DoubleNullObject.class)) {
                assertEquals(new DoubleNullObject(1.9), carpetReader.read());
                assertEquals(new DoubleNullObject(null), carpetReader.read());
            }
        }

        @Test
        void floatNullObject() throws IOException {
            Schema schema = schemaType("FloatNullObject").optionalFloat("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1.9f);
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record FloatNullObject(Float value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(FloatNullObject.class)) {
                assertEquals(new FloatNullObject(1.9f), carpetReader.read());
                assertEquals(new FloatNullObject(null), carpetReader.read());
            }
        }

        @Test
        void shortNullObject() throws IOException {
            Schema schema = schemaType("ShortNullObject").optionalInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record ShortNullObject(Short value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(ShortNullObject.class)) {
                assertEquals(new ShortNullObject((short) 1), carpetReader.read());
                assertEquals(new ShortNullObject(null), carpetReader.read());
            }
        }

        @Test
        void byteNullObject() throws IOException {
            Schema schema = schemaType("ByteNullObject").optionalInt("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", 1);
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record ByteNullObject(Byte value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(ByteNullObject.class)) {
                assertEquals(new ByteNullObject((byte) 1), carpetReader.read());
                assertEquals(new ByteNullObject(null), carpetReader.read());
            }
        }

        @Test
        void booleanNullObject() throws IOException {
            Schema schema = schemaType("BooleanNullObject").optionalBoolean("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", true);
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record BooleanNullObject(Boolean value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(BooleanNullObject.class)) {
                assertEquals(new BooleanNullObject(true), carpetReader.read());
                assertEquals(new BooleanNullObject(null), carpetReader.read());
            }
        }

        @Test
        void stringNullObject() throws IOException {
            Schema schema = schemaType("StringNullObject").optionalString("value").endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", "Madrid");
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record StringNullObject(String value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(StringNullObject.class)) {
                assertEquals(new StringNullObject("Madrid"), carpetReader.read());
                assertEquals(new StringNullObject(null), carpetReader.read());
            }
        }

        @Test
        void enumNullObject() throws IOException {
            Schema schema = schemaType("EnumNullObject").name("value").type().nullable()
                    .enumeration("Category").symbols("one", "two", "three").noDefault()
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", "one");
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            });

            record EnumNullObject(Category value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(EnumNullObject.class)) {
                assertEquals(new EnumNullObject(Category.one), carpetReader.read());
                assertEquals(new EnumNullObject(null), carpetReader.read());
            }
        }

    }

    @Nested
    class DateTypes {

        @Test
        void localDate() throws IOException {
            Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            Schema schema = SchemaBuilder.builder().record("LocalDate").fields()
                    .name("value").type(dateType).noDefault()
                    .endRecord();

            GenericData genericDataModel = new GenericData();
            genericDataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
            var readerTest = new ParquetReaderTest(schema);
            readerTest.writerWithModel(genericDataModel, writer -> {
                Record record = new Record(schema);
                record.put("value", LocalDate.of(2022, 11, 21));
                writer.write(record);
                record = new Record(schema);
                record.put("value", LocalDate.of(1976, 1, 15));
                writer.write(record);
            });

            record LocalDateRecord(LocalDate value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(LocalDateRecord.class)) {
                assertEquals(new LocalDateRecord(LocalDate.of(2022, 11, 21)), carpetReader.read());
                assertEquals(new LocalDateRecord(LocalDate.of(1976, 1, 15)), carpetReader.read());
            }
        }

        @Nested
        class TimeTypes {

            @Test
            void localTimeMillis() throws IOException {
                Schema timeType = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
                Schema schema = SchemaBuilder.builder().record("LocalTime").fields()
                        .name("value").type(timeType).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
                readerTest.writerWithModel(genericDataModel, writer -> {
                    Record record = new Record(schema);
                    record.put("value", LocalTime.of(19, 12, 21, 123456789));
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", LocalTime.of(23, 59, 59));
                    writer.write(record);
                });

                record LocalTimeRecord(LocalTime value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(LocalTimeRecord.class)) {
                    assertEquals(new LocalTimeRecord(LocalTime.of(19, 12, 21, 123000000)), carpetReader.read());
                    assertEquals(new LocalTimeRecord(LocalTime.of(23, 59, 59)), carpetReader.read());
                }
            }

            @Test
            void localTimeMicros() throws IOException {
                Schema timeType = LogicalTypes.timeMicros()
                        .addToSchema(Schema.create(Schema.Type.LONG));
                Schema schema = SchemaBuilder.builder().record("LocalTime").fields()
                        .name("value").type(timeType).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
                readerTest.writerWithModel(genericDataModel, writer -> {
                    Record record = new Record(schema);
                    record.put("value", LocalTime.of(19, 12, 21, 123456789));
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", LocalTime.of(23, 59, 59));
                    writer.write(record);
                });

                record LocalTimeRecord(LocalTime value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(LocalTimeRecord.class)) {
                    assertEquals(new LocalTimeRecord(LocalTime.of(19, 12, 21, 123456000)), carpetReader.read());
                    assertEquals(new LocalTimeRecord(LocalTime.of(23, 59, 59)), carpetReader.read());
                }
            }

        }

        @Nested
        class DateTimeTypes {

            @Test
            @Disabled("Waiting for 1.14.0 release")
            void localDateTimeMillis() throws IOException {
                Schema timeType = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                Schema schema = SchemaBuilder.builder().record("LocalDateTime").fields()
                        .name("value").type(timeType).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
                readerTest.writerWithModel(genericDataModel, writer -> {
                    Record record = new Record(schema);
                    record.put("value", LocalDateTime.of(2024, 5, 2, 19, 52, 21, 987654321));
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", LocalDateTime.of(1976, 1, 15, 0, 14, 10, 123456789));
                    writer.write(record);
                });

                record LocalDateTimeRecord(LocalDateTime value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(LocalDateTimeRecord.class)) {
                    assertEquals(new LocalDateTimeRecord(LocalDateTime.of(2024, 5, 2, 19, 52, 21, 987000000)),
                            carpetReader.read());
                    assertEquals(new LocalDateTimeRecord(LocalDateTime.of(1976, 1, 15, 0, 14, 10, 123000000)),
                            carpetReader.read());
                }
            }

            @Test
            @Disabled("Waiting for 1.14.0 release")
            void localDateTimeMicros() throws IOException {
                Schema timeType = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                Schema schema = SchemaBuilder.builder().record("LocalDateTime").fields()
                        .name("value").type(timeType).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
                readerTest.writerWithModel(genericDataModel, writer -> {
                    Record record = new Record(schema);
                    record.put("value", LocalDateTime.of(2024, 5, 2, 19, 52, 21, 987654321));
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", LocalDateTime.of(1976, 1, 15, 0, 14, 10, 123456789));
                    writer.write(record);
                });

                record LocalDateTimeRecord(LocalDateTime value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(LocalDateTimeRecord.class)) {
                    assertEquals(new LocalDateTimeRecord(LocalDateTime.of(2024, 5, 2, 19, 52, 21, 987654000)),
                            carpetReader.read());
                    assertEquals(new LocalDateTimeRecord(LocalDateTime.of(1976, 1, 15, 0, 14, 10, 123456000)),
                            carpetReader.read());
                }
            }

            @Test
            void instantMillis() throws IOException {
                Schema timeType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                Schema schema = SchemaBuilder.builder().record("Instant").fields()
                        .name("value").type(timeType).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
                readerTest.writerWithModel(genericDataModel, writer -> {
                    Record record = new Record(schema);
                    LocalDateTime local1 = LocalDateTime.of(2024, 5, 2, 19, 52, 21);
                    record.put("value", Instant.ofEpochSecond(local1.toEpochSecond(ZoneOffset.ofHours(1)), 987654321));
                    writer.write(record);
                    record = new Record(schema);
                    LocalDateTime local2 = LocalDateTime.of(1976, 1, 15, 0, 14, 10);
                    record.put("value", Instant.ofEpochSecond(local2.toEpochSecond(ZoneOffset.ofHours(2)), 123456789));
                    writer.write(record);
                });

                record LocalDateTimeRecord(Instant value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(LocalDateTimeRecord.class)) {
                    LocalDateTime local1 = LocalDateTime.of(2024, 5, 2, 18, 52, 21);
                    assertEquals(
                            new LocalDateTimeRecord(
                                    Instant.ofEpochSecond(local1.toEpochSecond(ZoneOffset.UTC), 987000000)),
                            carpetReader.read());
                    LocalDateTime local2 = LocalDateTime.of(1976, 1, 14, 22, 14, 10);
                    assertEquals(
                            new LocalDateTimeRecord(
                                    Instant.ofEpochSecond(local2.toEpochSecond(ZoneOffset.UTC), 123000000)),
                            carpetReader.read());
                }
            }

            @Test
            void instantMicros() throws IOException {
                Schema timeType = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                Schema schema = SchemaBuilder.builder().record("Instant").fields()
                        .name("value").type(timeType).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
                readerTest.writerWithModel(genericDataModel, writer -> {
                    Record record = new Record(schema);
                    LocalDateTime local1 = LocalDateTime.of(2024, 5, 2, 19, 52, 21);
                    record.put("value", Instant.ofEpochSecond(local1.toEpochSecond(ZoneOffset.ofHours(1)), 987654321));
                    writer.write(record);
                    record = new Record(schema);
                    LocalDateTime local2 = LocalDateTime.of(1976, 1, 15, 0, 14, 10);
                    record.put("value", Instant.ofEpochSecond(local2.toEpochSecond(ZoneOffset.ofHours(2)), 123456789));
                    writer.write(record);
                });

                record LocalDateTimeRecord(Instant value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(LocalDateTimeRecord.class)) {
                    LocalDateTime local1 = LocalDateTime.of(2024, 5, 2, 18, 52, 21);
                    assertEquals(
                            new LocalDateTimeRecord(
                                    Instant.ofEpochSecond(local1.toEpochSecond(ZoneOffset.UTC), 987654000)),
                            carpetReader.read());
                    LocalDateTime local2 = LocalDateTime.of(1976, 1, 14, 22, 14, 10);
                    assertEquals(
                            new LocalDateTimeRecord(
                                    Instant.ofEpochSecond(local2.toEpochSecond(ZoneOffset.UTC), 123456000)),
                            carpetReader.read());
                }
            }

        }

    }

    @Test
    void simpleType() throws IOException {
        Schema schema = schemaType("SimpleType")
                .optionalString("name")
                .requiredLong("longPrimitive")
                .optionalLong("longObject")
                .requiredInt("intPrimitive")
                .optionalInt("intObject")
                .requiredInt("shortPrimitive")
                .optionalInt("shortObject")
                .requiredInt("bytePrimitive")
                .optionalInt("byteObject")
                .endRecord();

        var readerTest = new ParquetReaderTest(schema);
        readerTest.writer(writer -> {
            Record record = new Record(schema);
            record.put("name", "foo");
            record.put("longPrimitive", 19191929L);
            record.put("longObject", 230202020L);
            record.put("intPrimitive", 1);
            record.put("intObject", 2);
            record.put("shortPrimitive", 4);
            record.put("shortObject", 5);
            record.put("bytePrimitive", 123);
            record.put("byteObject", 19);
            writer.write(record);
            record = new Record(schema);
            record.put("name", null);
            record.put("longPrimitive", 22342342L);
            record.put("longObject", null);
            record.put("intPrimitive", 3);
            record.put("intObject", null);
            record.put("shortPrimitive", 6);
            record.put("shortObject", null);
            record.put("bytePrimitive", 7);
            record.put("byteObject", null);
            writer.write(record);
        });

        record SimpleType(String name, long longPrimitive, Long longObject, int intPrimitive, Integer intObject,
                short shortPrimitive, Short shortObject, byte bytePrimitive, Byte byteObject) {
        }
        var rec1 = new SimpleType("foo", 19191929L, 230202020L, 1, 2, (short) 4, (short) 5, (byte) 123, (byte) 19);
        var rec2 = new SimpleType(null, 22342342L, null, 3, null, (short) 6, null, (byte) 7, null);

        try (var carpetReader = readerTest.getCarpetReader(SimpleType.class)) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void decimalTypes() throws IOException {
        Schema schema = schemaType("DecimalTypes")
                .optionalString("name")
                .requiredDouble("doublePrimitive")
                .optionalDouble("doubleObject")
                .requiredFloat("floatPrimitive")
                .optionalFloat("floatObject")
                .endRecord();

        var readerTest = new ParquetReaderTest(schema);
        readerTest.writer(writer -> {
            Record record = new Record(schema);
            record.put("name", "foo");
            record.put("doublePrimitive", 1.2);
            record.put("doubleObject", 2.4);
            record.put("floatPrimitive", 3.5f);
            record.put("floatObject", 6.7f);
            writer.write(record);
            record = new Record(schema);
            record.put("name", "bar");
            record.put("doublePrimitive", 6744.2);
            record.put("doubleObject", null);
            record.put("floatPrimitive", 292.1f);
            record.put("floatObject", null);
            writer.write(record);
        });

        record DecimalTypes(String name, double doublePrimitive, Double doubleObject, float floatPrimitive,
                Float floatObject) {
        }
        var rec1 = new DecimalTypes("foo", 1.2, 2.4, 3.5f, 6.7f);
        var rec2 = new DecimalTypes("bar", 6744.2, null, 292.1f, null);

        try (var carpetReader = readerTest.getCarpetReader(DecimalTypes.class)) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void booleanType() throws IOException {
        Schema schema = schemaType("BooleanTypes")
                .optionalString("name")
                .requiredBoolean("booleanPrimitive")
                .optionalBoolean("booleanObject")
                .endRecord();

        var readerTest = new ParquetReaderTest(schema);
        readerTest.writer(writer -> {
            Record record = new Record(schema);
            record.put("name", "foo");
            record.put("booleanPrimitive", true);
            record.put("booleanObject", false);
            writer.write(record);
            record = new Record(schema);
            record.put("name", "bar");
            record.put("booleanPrimitive", false);
            record.put("booleanObject", null);
            writer.write(record);
        });

        record BooleanTypes(String name, boolean booleanPrimitive, Boolean booleanObject) {
        }
        var rec1 = new BooleanTypes("foo", true, false);
        var rec2 = new BooleanTypes("bar", false, null);

        try (var carpetReader = readerTest.getCarpetReader(BooleanTypes.class)) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void enumType() throws IOException {
        Schema schema = schemaType("EnumType")
                .optionalString("name")
                .name("category").type().nullable().enumeration("Category").symbols("one", "two", "three").noDefault()
                .endRecord();

        var readerTest = new ParquetReaderTest(schema);
        readerTest.writer(writer -> {
            Record record = new Record(schema);
            record.put("name", "foo");
            record.put("category", "one");
            writer.write(record);
            record = new Record(schema);
            record.put("name", "bar");
            record.put("category", null);
            writer.write(record);
        });

        record EnumType(String name, Category category) {
        }

        try (var carpetReader = readerTest.getCarpetReader(EnumType.class)) {
            assertEquals(new EnumType("foo", Category.one), carpetReader.read());
            assertEquals(new EnumType("bar", null), carpetReader.read());
        }
    }

    @Test
    void canProjectFields() throws IOException {
        Schema schema = schemaType("ToProject")
                .optionalString("id")
                .requiredInt("value")
                .requiredBoolean("active")
                .requiredDouble("amount")
                .optionalString("category")
                .endRecord();

        var readerTest = new ParquetReaderTest(schema);
        readerTest.writer(writer -> {
            Record record = new Record(schema);
            record.put("id", "foo");
            record.put("value", 1);
            record.put("active", true);
            record.put("amount", 10.2);
            record.put("category", "SMALL");
            writer.write(record);
            record = new Record(schema);
            record.put("id", "boo");
            record.put("value", 2);
            record.put("active", false);
            record.put("amount", 22.3);
            record.put("category", "BIG");
            writer.write(record);
        });

        record Projection1(String id, int value, double amount) {
        }

        try (var carpetReader = readerTest.getCarpetReader(Projection1.class)) {
            assertEquals(new Projection1("foo", 1, 10.2), carpetReader.read());
            assertEquals(new Projection1("boo", 2, 22.3), carpetReader.read());
        }

        record Projection2(String id, boolean active, String category) {
        }

        try (var carpetReader = readerTest.getCarpetReader(Projection2.class)) {
            assertEquals(new Projection2("foo", true, "SMALL"), carpetReader.read());
            assertEquals(new Projection2("boo", false, "BIG"), carpetReader.read());
        }
    }

    @Nested
    class NullabilityOnPrimitiveFields {

        record NotNullableFields(String id, int size, double value, float temperature, boolean active) {
        }

        private final Schema schema = schemaType("NotNullableFields")
                .requiredString("id")
                .optionalInt("size")
                .optionalDouble("value")
                .optionalFloat("temperature")
                .optionalBoolean("active")
                .endRecord();

        @Test
        void supportsOptionalColumnsOnPrimitiveFields() throws IOException {
            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("id", "foo");
                record.put("size", 1);
                record.put("value", 2.0);
                record.put("temperature", 3.0f);
                record.put("active", true);
                writer.write(record);
            });

            try (var carpetReader = readerTest.getCarpetReader(NotNullableFields.class)) {
                assertEquals(new NotNullableFields("foo", 1, 2.0, 3.0f, true), carpetReader.read());
            }
        }

        @Test
        void primitiveFieldsAreFilledWithDefaultValue() throws IOException {

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("id", "foo");
                record.put("size", null);
                record.put("value", null);
                record.put("temperature", null);
                record.put("active", null);
                writer.write(record);
            });

            try (var carpetReader = readerTest.getCarpetReader(NotNullableFields.class)) {
                assertEquals(new NotNullableFields("foo", 0, 0.0, 0.0f, false), carpetReader.read());
            }
        }

        @Test
        void failOnFullForPrimitivesFlag() throws IOException {
            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("id", "foo");
                record.put("size", null);
                record.put("value", null);
                record.put("temperature", null);
                record.put("active", null);
                writer.write(record);
            });

            try (var carpetReader = readerTest.getCarpetReader(NotNullableFields.class,
                    ReadFlag.FAIL_ON_NULL_FOR_PRIMITIVES)) {
                assertThrows(RecordTypeConversionException.class, () -> carpetReader.read());
            }
        }

    }

    @Nested
    class MissingColumns {

        private final Schema schema = schemaType("NotNullableFields")
                .requiredString("id")
                .optionalInt("size")
                .optionalBoolean("active")
                .endRecord();

        @Test
        void failOnMissingColumnsByDefault() throws IOException {
            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("id", "foo");
                record.put("size", 1);
                record.put("active", true);
                writer.write(record);
            });

            record WithMissingColumns(String id, int size, int missing1, double missing2) {
            }

            try (var carpetReader = readerTest.getCarpetReader(WithMissingColumns.class)) {
                assertThrows(CarpetMissingColumnException.class, () -> carpetReader.read());
            }
        }

        @Test
        void missingColumnsOnPrimitivesAreFilledWithDefaultTypeValue() throws IOException {
            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("id", "foo");
                record.put("size", 1);
                record.put("active", true);
                writer.write(record);
            });

            record WithMissingColumns(String id, int size,
                    int missing1, double missing2, float missing3, boolean missing4) {
            }

            try (var carpetReader = readerTest.getCarpetReader(WithMissingColumns.class,
                    ReadFlag.DONT_FAIL_ON_MISSING_COLUMN)) {
                assertEquals(new WithMissingColumns("foo", 1, 0, 0.0, 0.0f, false), carpetReader.read());
            }
        }

        @Test
        void missingColumnsOnObjectsAreFilledWithNull() throws IOException {
            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("id", "foo");
                record.put("size", 1);
                record.put("active", true);
                writer.write(record);
            });

            record WithMissingColumns(String id, int size,
                    Integer missing1, Double missing2, Float missing3, Boolean missing4) {
            }

            try (var carpetReader = readerTest.getCarpetReader(WithMissingColumns.class,
                    ReadFlag.DONT_FAIL_ON_MISSING_COLUMN)) {
                assertEquals(new WithMissingColumns("foo", 1, null, null, null, null), carpetReader.read());
            }
        }
    }

    @Test
    void nestedRecord() throws IOException {
        Schema nestedSchema = schemaType("Nested").optionalString("id").requiredInt("value").endRecord();
        Schema schema = schemaType("NestedRecord")
                .optionalString("name")
                .name("nested").type().optional().type(nestedSchema)
                .endRecord();

        var readerTest = new ParquetReaderTest(schema);
        readerTest.writer(writer -> {
            Record nested = new Record(nestedSchema);
            nested.put("id", "Madrid");
            nested.put("value", 10);
            Record record = new Record(schema);
            record.put("name", "foo");
            record.put("nested", nested);
            writer.write(record);
            record = new Record(schema);
            record.put("name", "bar");
            record.put("nested", null);
            writer.write(record);
        });

        record Nested(String id, int value) {
        }

        record NestedRecord(String name, Nested nested) {
        }

        var rec1 = new NestedRecord("foo", new Nested("Madrid", 10));
        var rec2 = new NestedRecord("bar", null);
        try (var carpetReader = readerTest.getCarpetReader(NestedRecord.class)) {
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Test
    void projectNestedRecord() throws IOException {
        Schema nestedSchema = schemaType("Nested")
                .optionalString("id").requiredInt("value").requiredDouble("amount").endRecord();
        Schema schema = schemaType("NestedRecord")
                .optionalString("name")
                .requiredBoolean("active")
                .name("nested").type().optional().type(nestedSchema)
                .endRecord();

        var readerTest = new ParquetReaderTest(schema);
        readerTest.writer(writer -> {
            Record nested = new Record(nestedSchema);
            nested.put("id", "Madrid");
            nested.put("value", 10);
            nested.put("amount", 20.0);
            Record record = new Record(schema);
            record.put("name", "foo");
            record.put("active", true);
            record.put("nested", nested);
            writer.write(record);
            record = new Record(schema);
            record.put("name", "bar");
            record.put("active", false);
            record.put("nested", null);
            writer.write(record);
        });

        record ProjectedNested(String id, int value) {
        }

        record ProjectedNestedRecord(String name, ProjectedNested nested) {
        }

        try (var carpetReader = readerTest.getCarpetReader(ProjectedNestedRecord.class)) {
            var expectedRec1 = new ProjectedNestedRecord("foo", new ProjectedNested("Madrid", 10));
            var expectedRec2 = new ProjectedNestedRecord("bar", null);
            assertEquals(expectedRec1, carpetReader.read());
            assertEquals(expectedRec2, carpetReader.read());
        }

        record ProjectedNested2(int value, double amount) {
        }

        record ProjectedNestedRecord2(boolean active, ProjectedNested2 nested) {
        }

        try (var carpetReader = readerTest.getCarpetReader(ProjectedNestedRecord2.class)) {
            var expectedRec1 = new ProjectedNestedRecord2(true, new ProjectedNested2(10, 20.0));
            var expectedRec2 = new ProjectedNestedRecord2(false, null);
            assertEquals(expectedRec1, carpetReader.read());
            assertEquals(expectedRec2, carpetReader.read());
        }
    }

    @Nested
    class OneLevelCollection {

        @Test
        void collectionPrimitive() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.ONE);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionFilteredByProjection() throws IOException {

            record CollectionPrimitive(String name, int size, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", 10, List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", 0, null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.ONE);
            writerTest.write(rec1, rec2);

            record CollectionFiltered(String name, int size) {
            }

            var expectedRec1 = new CollectionFiltered("foo", 10);
            var expectedRec2 = new CollectionFiltered("bar", 0);

            var reader = writerTest.getCarpetReader(CollectionFiltered.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionPrimitiveNullValuesNotSupported() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", asList(1, null, 3));
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.ONE);
            assertThrows(NullPointerException.class, () -> writerTest.write(rec1));
        }

        @Test
        void collectionPrimitiveString() throws IOException {

            record CollectionPrimitiveString(String name, List<String> sizes) {
            }

            var rec1 = new CollectionPrimitiveString("foo", List.of("1", "2", "3"));
            var rec2 = new CollectionPrimitiveString("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveString.class)
                    .withLevel(AnnotatedLevels.ONE);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionPrimitiveEnum() throws IOException {

            record CollectionPrimitiveEnum(String name, List<Category> category) {
            }

            var rec1 = new CollectionPrimitiveEnum("foo", asList(Category.one, Category.two, Category.three));
            var rec2 = new CollectionPrimitiveEnum("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveEnum.class)
                    .withLevel(AnnotatedLevels.ONE);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo", List.of(new ChildItem("1", false), new ChildItem("2", true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class)
                    .withLevel(AnnotatedLevels.ONE);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionCompositeProjected() throws IOException {

            record ChildItem(String id, int size, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo", List.of(
                    new ChildItem("1", 10, false),
                    new ChildItem("2", 20, true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class)
                    .withLevel(AnnotatedLevels.ONE);
            writerTest.write(rec1, rec2);

            record ChildItemProjected(int size, String id) {
            }

            record CollectionProjected(String name, List<ChildItemProjected> status) {
            }

            var expectedRec1 = new CollectionProjected("foo",
                    List.of(new ChildItemProjected(10, "1"),
                            new ChildItemProjected(20, "2")));
            var expectedRec2 = new CollectionProjected("bar", null);

            var reader = writerTest.getCarpetReader(CollectionProjected.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionNestedMap() throws IOException {

            record CollectionNestedMap(String name, List<Map<String, Boolean>> status) {
            }

            var rec1 = new CollectionNestedMap("even", List.of(
                    Map.of("1", false, "2", true),
                    Map.of("3", false, "4", true)));
            var rec2 = new CollectionNestedMap("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedMap.class)
                    .withLevel(AnnotatedLevels.ONE);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionPrimitive() throws IOException {

            record CollectionNestedCollectionPrimitive(String name, List<List<Integer>> status) {
            }

            var rec1 = new CollectionNestedCollectionPrimitive("foo", List.of(List.of(1, 2, 3)));
            var rec2 = new CollectionNestedCollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.ONE);
            assertThrows(RecordTypeConversionException.class, () -> writerTest.write(rec1, rec2));
        }

        @Test
        void collectionNestedCollectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionNestedCollectionComposite(String name, List<List<ChildItem>> status) {
            }

            var rec1 = new CollectionNestedCollectionComposite("foo",
                    List.of(List.of(new ChildItem("1", false), new ChildItem("2", true))));
            var rec2 = new CollectionNestedCollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionComposite.class)
                    .withLevel(AnnotatedLevels.ONE);
            assertThrows(RecordTypeConversionException.class, () -> writerTest.write(rec1, rec2));
        }

        @Test
        void setCollection() throws IOException {

            record SetCollection(String name, Set<String> sizes) {
            }

            var rec1 = new SetCollection("foo", Set.of("1", "2", "3"));
            var writerTest = new ParquetWriterTest<>(SetCollection.class)
                    .withLevel(AnnotatedLevels.ONE);
            writerTest.write(rec1);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
        }

        @Nested
        class ListConversion {

            record ListCollection(String name, List<String> sizes) {
            }

            private final ParquetWriterTest<ListCollection> writerTest = new ParquetWriterTest<>(ListCollection.class)
                    .withLevel(AnnotatedLevels.ONE);
            private final List<String> sizes = List.of("1", "2", "3");

            @BeforeEach
            void givenPersistedCollection() throws IOException {
                writerTest.write(new ListCollection("foo", sizes));
            }

            @Test
            void genericCollection() throws IOException {

                record GenericCollection(String name, Collection<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericCollection.class);
                assertEquals(new GenericCollection("foo", sizes), reader.read());
            }

            @Test
            void genericList() throws IOException {

                record GenericList(String name, List<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericList.class);
                assertEquals(new GenericList("foo", sizes), reader.read());
            }

            @Test
            void arrayListCollection() throws IOException {

                record ArrayListCollection(String name, ArrayList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(ArrayListCollection.class);
                var read = reader.read();
                assertEquals(new ArrayListCollection("foo", new ArrayList<>(sizes)), read);
                assertTrue(read.sizes instanceof ArrayList);
            }

            @Test
            void linkedListCollection() throws IOException {

                record LinkedListCollection(String name, LinkedList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(LinkedListCollection.class);
                var read = reader.read();
                assertEquals(new LinkedListCollection("foo", new LinkedList<>(sizes)), read);
                assertTrue(read.sizes instanceof LinkedList);
            }

            @Test
            void unknownSetCollection() throws IOException {

                record UnknownCollection(String name, CopyOnWriteArrayList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(UnknownCollection.class);
                var read = reader.read();
                assertEquals(new UnknownCollection("foo", new CopyOnWriteArrayList<>(sizes)), read);
                assertTrue(read.sizes instanceof CopyOnWriteArrayList);
            }

        }

        @Nested
        class SetConversion {

            record ListCollection(String name, List<String> sizes) {
            }

            private final ParquetWriterTest<ListCollection> writerTest = new ParquetWriterTest<>(ListCollection.class)
                    .withLevel(AnnotatedLevels.ONE);
            private final List<String> sizes = List.of("1", "2", "3");

            @BeforeEach
            void givenPersistedCollection() throws IOException {
                writerTest.write(new ListCollection("foo", sizes));
            }

            @Test
            void genericSetCollection() throws IOException {

                record GenericSetCollection(String name, Set<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericSetCollection.class);
                assertEquals(new GenericSetCollection("foo", new HashSet<>(sizes)), reader.read());
            }

            @Test
            void hashSetCollection() throws IOException {

                record HashSetCollection(String name, HashSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(HashSetCollection.class);
                var read = reader.read();
                assertEquals(new HashSetCollection("foo", new HashSet<>(sizes)), read);
                assertTrue(read.sizes instanceof HashSet);
            }

            @Test
            void linkedHashSetCollection() throws IOException {

                record LinkedHashSetCollection(String name, LinkedHashSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(LinkedHashSetCollection.class);
                var read = reader.read();
                assertEquals(new LinkedHashSetCollection("foo", new LinkedHashSet<>(sizes)), read);
                assertTrue(read.sizes instanceof LinkedHashSet);
            }

            @Test
            void unknownSetCollection() throws IOException {

                record UnknownSetCollection(String name, TreeSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(UnknownSetCollection.class);
                var read = reader.read();
                assertEquals(new UnknownSetCollection("foo", new TreeSet<>(sizes)), read);
                assertTrue(read.sizes instanceof TreeSet);
            }

        }
    }

    @Nested
    class TwoLevelCollection {

        @Test
        void collectionPrimitive() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionFilteredByProjection() throws IOException {

            record CollectionPrimitive(String name, int size, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", 10, List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", 0, null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            record CollectionFiltered(String name, int size) {
            }

            var expectedRec1 = new CollectionFiltered("foo", 10);
            var expectedRec2 = new CollectionFiltered("bar", 0);

            var reader = writerTest.getCarpetReader(CollectionFiltered.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionPrimitiveNullValuesNotSupported() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", asList(1, null, 3));
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.TWO);
            assertThrows(NullPointerException.class, () -> writerTest.write(rec1));
        }

        @Test
        void collectionPrimitiveString() throws IOException {

            record CollectionPrimitiveString(String name, List<String> sizes) {
            }

            var rec1 = new CollectionPrimitiveString("foo", List.of("1", "2", "3"));
            var rec2 = new CollectionPrimitiveString("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveString.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionPrimitiveEnum() throws IOException {

            record CollectionPrimitiveEnum(String name, List<Category> category) {
            }

            var rec1 = new CollectionPrimitiveEnum("foo", asList(Category.one, Category.two, Category.three));
            var rec2 = new CollectionPrimitiveEnum("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveEnum.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo", List.of(new ChildItem("1", false), new ChildItem("2", true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionCompositeProjected() throws IOException {

            record ChildItem(String id, int size, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo", List.of(
                    new ChildItem("1", 10, false),
                    new ChildItem("2", 20, true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            record ChildItemProjected(int size, String id) {
            }

            record CollectionProjected(String name, List<ChildItemProjected> status) {
            }

            var expectedRec1 = new CollectionProjected("foo",
                    List.of(new ChildItemProjected(10, "1"),
                            new ChildItemProjected(20, "2")));
            var expectedRec2 = new CollectionProjected("bar", null);

            var reader = writerTest.getCarpetReader(CollectionProjected.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionNestedMap() throws IOException {

            record CollectionNestedMap(String name, List<Map<String, Boolean>> status) {
            }

            var rec1 = new CollectionNestedMap("even", List.of(
                    Map.of("1", false, "2", true),
                    Map.of("3", false, "4", true)));
            var rec2 = new CollectionNestedMap("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedMap.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionPrimitive() throws IOException {

            record CollectionNestedCollectionPrimitive(String name, List<List<Integer>> status) {
            }

            var rec1 = new CollectionNestedCollectionPrimitive("foo", List.of(List.of(1, 2, 3)));
            var rec2 = new CollectionNestedCollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionPrimitive.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionNestedCollectionComposite(String name, List<List<ChildItem>> status) {
            }

            var rec1 = new CollectionNestedCollectionComposite("foo",
                    List.of(List.of(new ChildItem("1", false), new ChildItem("2", true))));
            var rec2 = new CollectionNestedCollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionComposite.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void setCollection() throws IOException {

            record SetCollection(String name, Set<String> sizes) {
            }

            var rec1 = new SetCollection("foo", Set.of("1", "2", "3"));
            var writerTest = new ParquetWriterTest<>(SetCollection.class)
                    .withLevel(AnnotatedLevels.TWO);
            writerTest.write(rec1);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
        }

        @Nested
        class ListConversion {

            record ListCollection(String name, List<String> sizes) {
            }

            private final ParquetWriterTest<ListCollection> writerTest = new ParquetWriterTest<>(ListCollection.class)
                    .withLevel(AnnotatedLevels.TWO);
            private final List<String> sizes = List.of("1", "2", "3");

            @BeforeEach
            void givenPersistedCollection() throws IOException {
                writerTest.write(new ListCollection("foo", sizes));
            }

            @Test
            void genericCollection() throws IOException {

                record GenericCollection(String name, Collection<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericCollection.class);
                assertEquals(new GenericCollection("foo", sizes), reader.read());
            }

            @Test
            void genericList() throws IOException {

                record GenericList(String name, List<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericList.class);
                assertEquals(new GenericList("foo", sizes), reader.read());
            }

            @Test
            void arrayListCollection() throws IOException {

                record ArrayListCollection(String name, ArrayList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(ArrayListCollection.class);
                var read = reader.read();
                assertEquals(new ArrayListCollection("foo", new ArrayList<>(sizes)), read);
                assertTrue(read.sizes instanceof ArrayList);
            }

            @Test
            void linkedListCollection() throws IOException {

                record LinkedListCollection(String name, LinkedList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(LinkedListCollection.class);
                var read = reader.read();
                assertEquals(new LinkedListCollection("foo", new LinkedList<>(sizes)), read);
                assertTrue(read.sizes instanceof LinkedList);
            }

            @Test
            void unknownSetCollection() throws IOException {

                record UnknownCollection(String name, CopyOnWriteArrayList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(UnknownCollection.class);
                var read = reader.read();
                assertEquals(new UnknownCollection("foo", new CopyOnWriteArrayList<>(sizes)), read);
                assertTrue(read.sizes instanceof CopyOnWriteArrayList);
            }

        }

        @Nested
        class SetConversion {

            record ListCollection(String name, List<String> sizes) {
            }

            private final ParquetWriterTest<ListCollection> writerTest = new ParquetWriterTest<>(ListCollection.class)
                    .withLevel(AnnotatedLevels.TWO);
            private final List<String> sizes = List.of("1", "2", "3");

            @BeforeEach
            void givenPersistedCollection() throws IOException {
                writerTest.write(new ListCollection("foo", sizes));
            }

            @Test
            void genericSetCollection() throws IOException {

                record GenericSetCollection(String name, Set<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericSetCollection.class);
                assertEquals(new GenericSetCollection("foo", new HashSet<>(sizes)), reader.read());
            }

            @Test
            void hashSetCollection() throws IOException {

                record HashSetCollection(String name, HashSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(HashSetCollection.class);
                var read = reader.read();
                assertEquals(new HashSetCollection("foo", new HashSet<>(sizes)), read);
                assertTrue(read.sizes instanceof HashSet);
            }

            @Test
            void linkedHashSetCollection() throws IOException {

                record LinkedHashSetCollection(String name, LinkedHashSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(LinkedHashSetCollection.class);
                var read = reader.read();
                assertEquals(new LinkedHashSetCollection("foo", new LinkedHashSet<>(sizes)), read);
                assertTrue(read.sizes instanceof LinkedHashSet);
            }

            @Test
            void unknownSetCollection() throws IOException {

                record UnknownSetCollection(String name, TreeSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(UnknownSetCollection.class);
                var read = reader.read();
                assertEquals(new UnknownSetCollection("foo", new TreeSet<>(sizes)), read);
                assertTrue(read.sizes instanceof TreeSet);
            }

        }
    }

    @Nested
    class ThreeLevelCollection {

        @Test
        void collectionPrimitive() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionFilteredByProjection() throws IOException {

            record CollectionPrimitive(String name, int size, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", 10, List.of(1, 2, 3));
            var rec2 = new CollectionPrimitive("bar", 0, null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class);
            writerTest.write(rec1, rec2);

            record CollectionFiltered(String name, int size) {
            }

            var expectedRec1 = new CollectionFiltered("foo", 10);
            var expectedRec2 = new CollectionFiltered("bar", 0);

            var reader = writerTest.getCarpetReader(CollectionFiltered.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionPrimitiveNullValues() throws IOException {

            record CollectionPrimitive(String name, List<Integer> sizes) {
            }

            var rec1 = new CollectionPrimitive("foo", asList(1, null, 3));
            var rec2 = new CollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitive.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionPrimitiveString() throws IOException {

            record CollectionPrimitiveString(String name, List<String> sizes) {
            }

            var rec1 = new CollectionPrimitiveString("foo", asList("1", null, "3"));
            var rec2 = new CollectionPrimitiveString("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveString.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionPrimitiveEnum() throws IOException {

            record CollectionPrimitiveEnum(String name, List<Category> category) {
            }

            var rec1 = new CollectionPrimitiveEnum("foo", asList(Category.one, null, Category.three));
            var rec2 = new CollectionPrimitiveEnum("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionPrimitiveEnum.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo",
                    asList(new ChildItem("1", false), null, new ChildItem("2", true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionCompositeProjected() throws IOException {

            record ChildItem(String id, int size, boolean active) {
            }

            record CollectionComposite(String name, List<ChildItem> status) {
            }

            var rec1 = new CollectionComposite("foo",
                    asList(new ChildItem("1", 10, false), null, new ChildItem("2", 20, true)));
            var rec2 = new CollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionComposite.class);
            writerTest.write(rec1, rec2);

            record ChildItemProjected(int size, String id) {
            }

            record CollectionProjected(String name, List<ChildItemProjected> status) {
            }

            var expectedRec1 = new CollectionProjected("foo",
                    asList(new ChildItemProjected(10, "1"),
                            null,
                            new ChildItemProjected(20, "2")));
            var expectedRec2 = new CollectionProjected("bar", null);

            var reader = writerTest.getCarpetReader(CollectionProjected.class);
            assertEquals(expectedRec1, reader.read());
            assertEquals(expectedRec2, reader.read());
        }

        @Test
        void collectionNestedMap() throws IOException {

            record CollectionNestedMap(String name, List<Map<String, Boolean>> status) {
            }

            var rec1 = new CollectionNestedMap("even",
                    asList(Map.of("1", false, "2", true),
                            null,
                            Map.of("3", false, "4", true)));
            var rec2 = new CollectionNestedMap("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedMap.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionPrimitive() throws IOException {

            record CollectionNestedCollectionPrimitive(String name, List<List<Integer>> status) {
            }

            var rec1 = new CollectionNestedCollectionPrimitive("foo", asList(asList(1, null, 3), null));
            var rec2 = new CollectionNestedCollectionPrimitive("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionPrimitive.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void collectionNestedCollectionComposite() throws IOException {

            record ChildItem(String id, boolean active) {
            }

            record CollectionNestedCollectionComposite(String name, List<List<ChildItem>> status) {
            }

            var rec1 = new CollectionNestedCollectionComposite("foo",
                    asList(
                            asList(
                                    new ChildItem("1", false),
                                    null,
                                    new ChildItem("2", true)),
                            null));
            var rec2 = new CollectionNestedCollectionComposite("bar", null);
            var writerTest = new ParquetWriterTest<>(CollectionNestedCollectionComposite.class);
            writerTest.write(rec1, rec2);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void setCollection() throws IOException {

            record SetCollection(String name, Set<String> sizes) {
            }

            var rec1 = new SetCollection("foo", Set.of("1", "2", "3"));
            var writerTest = new ParquetWriterTest<>(SetCollection.class);
            writerTest.write(rec1);

            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
        }

        @Nested
        class ListConversion {

            record ListCollection(String name, List<String> sizes) {
            }

            private final ParquetWriterTest<ListCollection> writerTest = new ParquetWriterTest<>(ListCollection.class);
            private final List<String> sizes = List.of("1", "2", "3");

            @BeforeEach
            void givenPersistedCollection() throws IOException {
                writerTest.write(new ListCollection("foo", sizes));
            }

            @Test
            void genericCollection() throws IOException {

                record GenericCollection(String name, Collection<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericCollection.class);
                assertEquals(new GenericCollection("foo", sizes), reader.read());
            }

            @Test
            void genericList() throws IOException {

                record GenericList(String name, List<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericList.class);
                assertEquals(new GenericList("foo", sizes), reader.read());
            }

            @Test
            void arrayListCollection() throws IOException {

                record ArrayListCollection(String name, ArrayList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(ArrayListCollection.class);
                var read = reader.read();
                assertEquals(new ArrayListCollection("foo", new ArrayList<>(sizes)), read);
                assertTrue(read.sizes instanceof ArrayList);
            }

            @Test
            void linkedListCollection() throws IOException {

                record LinkedListCollection(String name, LinkedList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(LinkedListCollection.class);
                var read = reader.read();
                assertEquals(new LinkedListCollection("foo", new LinkedList<>(sizes)), read);
                assertTrue(read.sizes instanceof LinkedList);
            }

            @Test
            void unknownSetCollection() throws IOException {

                record UnknownCollection(String name, CopyOnWriteArrayList<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(UnknownCollection.class);
                var read = reader.read();
                assertEquals(new UnknownCollection("foo", new CopyOnWriteArrayList<>(sizes)), read);
                assertTrue(read.sizes instanceof CopyOnWriteArrayList);
            }

        }

        @Nested
        class SetConversion {

            record ListCollection(String name, List<String> sizes) {
            }

            private final ParquetWriterTest<ListCollection> writerTest = new ParquetWriterTest<>(ListCollection.class);
            private final List<String> sizes = List.of("1", "2", "3");

            @BeforeEach
            void givenPersistedCollection() throws IOException {
                writerTest.write(new ListCollection("foo", sizes));
            }

            @Test
            void genericSetCollection() throws IOException {

                record GenericSetCollection(String name, Set<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericSetCollection.class);
                assertEquals(new GenericSetCollection("foo", new HashSet<>(sizes)), reader.read());
            }

            @Test
            void hashSetCollection() throws IOException {

                record HashSetCollection(String name, HashSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(HashSetCollection.class);
                var read = reader.read();
                assertEquals(new HashSetCollection("foo", new HashSet<>(sizes)), read);
                assertTrue(read.sizes instanceof HashSet);
            }

            @Test
            void linkedHashSetCollection() throws IOException {

                record LinkedHashSetCollection(String name, LinkedHashSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(LinkedHashSetCollection.class);
                var read = reader.read();
                assertEquals(new LinkedHashSetCollection("foo", new LinkedHashSet<>(sizes)), read);
                assertTrue(read.sizes instanceof LinkedHashSet);
            }

            @Test
            void unknownSetCollection() throws IOException {

                record UnknownSetCollection(String name, TreeSet<String> sizes) {
                }

                var reader = writerTest.getCarpetReader(UnknownSetCollection.class);
                var read = reader.read();
                assertEquals(new UnknownSetCollection("foo", new TreeSet<>(sizes)), read);
                assertTrue(read.sizes instanceof TreeSet);
            }

        }
    }

    @Nested
    class Maps {

        @Test
        void nestedMapStringKeyPrimitiveValue() throws IOException {

            record NestedMapStringKeyPrimitiveValue(String name, Map<String, Integer> sizes) {
            }

            Map<String, Integer> map = new HashMap<>(Map.of("one", 1, "three", 3));
            map.put("two", null);
            var rec1 = new NestedMapStringKeyPrimitiveValue("foo", map);
            var rec2 = new NestedMapStringKeyPrimitiveValue("bar", null);
            var writerTest = new ParquetWriterTest<>(NestedMapStringKeyPrimitiveValue.class);
            writerTest.write(rec1, rec2);
            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void nestedMapStringKeyStringValue() throws IOException {

            record NestedMapStringKeyStringValue(String name, Map<String, String> sizes) {
            }

            Map<String, String> map = new HashMap<>(Map.of("one", "1", "three", "3"));
            map.put("two", null);
            var rec1 = new NestedMapStringKeyStringValue("foo", map);
            var rec2 = new NestedMapStringKeyStringValue("bar", null);
            var writerTest = new ParquetWriterTest<>(NestedMapStringKeyStringValue.class);
            writerTest.write(rec1, rec2);
            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void nestedMapPrimitiveKeyRecordValue() throws IOException {

            record ChildMap(String id, double value) {
            }
            record NestedMapPrimitiveKeyRecordValue(String name, Map<Integer, ChildMap> metrics) {
            }

            Map<Integer, ChildMap> map = new HashMap<>(Map.of(1, new ChildMap("Madrid", 12.0),
                    3, new ChildMap("Bilbao", 23.0)));
            map.put(2, null);
            var rec1 = new NestedMapPrimitiveKeyRecordValue("foo", map);
            var rec2 = new NestedMapPrimitiveKeyRecordValue("bar", null);
            var writerTest = new ParquetWriterTest<>(NestedMapPrimitiveKeyRecordValue.class);
            writerTest.write(rec1, rec2);
            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void nestedMapPrimitiveKeyListPrimitiveValue() throws IOException {

            record NestedMapPrimitiveKeyListPrimitiveValue(String name, Map<Short, List<Integer>> metrics) {
            }

            Map<Short, List<Integer>> map = new HashMap<>(Map.of((short) 1, List.of(1, 2, 3),
                    (short) 3, List.of(4, 5, 6)));
            map.put((short) 2, null);
            var rec1 = new NestedMapPrimitiveKeyListPrimitiveValue("foo", map);
            var rec2 = new NestedMapPrimitiveKeyListPrimitiveValue("bar", null);
            var writerTest = new ParquetWriterTest<>(NestedMapPrimitiveKeyListPrimitiveValue.class);
            writerTest.write(rec1, rec2);
            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Test
        void nestedMapRecordKeyMapValue() throws IOException {

            record CompositeKey(String a, String b) {
            }

            record NestedMapRecordKeyMapValue(String name, Map<CompositeKey, Map<Category, String>> metrics) {
            }

            Map<CompositeKey, Map<Category, String>> map = new HashMap<>(Map.of(
                    new CompositeKey("A", "B"), Map.of(Category.one, "ONE", Category.two, "TWO")));
            map.put(new CompositeKey("B", "C"), null);
            var rec1 = new NestedMapRecordKeyMapValue("foo", map);
            var rec2 = new NestedMapRecordKeyMapValue("bar", null);
            var writerTest = new ParquetWriterTest<>(NestedMapRecordKeyMapValue.class);
            writerTest.write(rec1, rec2);
            var reader = writerTest.getCarpetReader();
            assertEquals(rec1, reader.read());
            assertEquals(rec2, reader.read());
        }

        @Nested
        class MapConversion {

            record NestedMap(String name, Map<String, Integer> sizes) {
            }

            private final ParquetWriterTest<NestedMap> writerTest = new ParquetWriterTest<>(NestedMap.class);
            private final Map<String, Integer> values = Map.of("one", 1, "three", 3);

            @BeforeEach
            void setup() throws IOException {
                var rec1 = new NestedMap("foo", values);
                writerTest.write(rec1);
            }

            @Test
            void genericMap() throws IOException {

                record GenericMap(String name, Map<String, Integer> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericMap.class);
                assertEquals(new GenericMap("foo", values), reader.read());
            }

            @Test
            void hashMap() throws IOException {

                record GenericMap(String name, HashMap<String, Integer> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericMap.class);
                GenericMap read = reader.read();
                assertEquals(new GenericMap("foo", new HashMap<>(values)), read);
                assertTrue(read.sizes() instanceof HashMap);
            }

            @Test
            void linkedHashMap() throws IOException {

                record GenericMap(String name, LinkedHashMap<String, Integer> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericMap.class);
                GenericMap read = reader.read();
                assertEquals(new GenericMap("foo", new LinkedHashMap<>(values)), read);
                assertTrue(read.sizes() instanceof LinkedHashMap);
            }

            @Test
            void unknownMap() throws IOException {

                record GenericMap(String name, ConcurrentHashMap<String, Integer> sizes) {
                }

                var reader = writerTest.getCarpetReader(GenericMap.class);
                GenericMap read = reader.read();
                assertEquals(new GenericMap("foo", new ConcurrentHashMap<>(values)), read);
                assertTrue(read.sizes() instanceof ConcurrentHashMap);
            }

        }
    }

    @Nested
    class FieldNameMapping {

        @Test
        void canHaveDifferentOrder() throws IOException {

            record SomeRecord(int a, long b, String c, double d) {
            }

            var writerTest = new ParquetWriterTest<>(SomeRecord.class);
            writerTest.write(new SomeRecord(1, 2L, "A", 3.0));

            record ReverseRecord(double d, String c, long b, int a) {
            }

            var reader = writerTest.getCarpetReader(ReverseRecord.class);
            assertEquals(new ReverseRecord(3.0, "A", 2L, 1), reader.read());
        }

        @Nested
        class SnakeCaseConversion {

            @Test
            void canNotMapSnakeCaseColumnNameIfDisabled() throws IOException {

                record SanakeCaseField(int some_value) {
                }

                var writerTest = new ParquetWriterTest<>(SanakeCaseField.class);
                writerTest.write(new SanakeCaseField(1));

                record CamelCaseField(int someValue) {
                }

                var reader = new CarpetReader<>(writerTest.getTestFile(), CamelCaseField.class)
                        .withFieldMatchingStrategy(FIELD_NAME);
                assertThrows(CarpetMissingColumnException.class, () -> reader.toList());
            }

            @Test
            void mapSnakeCaseColumnNameWhenEnabled() throws IOException {

                record SanakeCaseField(int some_value) {
                }

                var writerTest = new ParquetWriterTest<>(SanakeCaseField.class);
                writerTest.write(new SanakeCaseField(1));

                record CamelCaseField(int someValue) {
                }

                var reader = new CarpetReader<>(writerTest.getTestFile(), CamelCaseField.class)
                        .withFieldMatchingStrategy(SNAKE_CASE);
                CamelCaseField value = reader.iterator().next();
                assertEquals(new CamelCaseField(1), value);
            }

            @Test
            void bestEffortPriorityOnMatchingNameColumnsOverSnakeCase() throws IOException {

                record SanakeCaseField(int some_value, double someValue) {
                }

                var writerTest = new ParquetWriterTest<>(SanakeCaseField.class);
                writerTest.write(new SanakeCaseField(1, 2.0));

                record CamelCaseField(int some_value, double someValue) {
                }

                var reader = new CarpetReader<>(writerTest.getTestFile(), CamelCaseField.class)
                        .withFieldMatchingStrategy(BEST_EFFORT);
                CamelCaseField value = reader.iterator().next();
                assertEquals(new CamelCaseField(1, 2.0), value);
            }

            @Test
            void priorityOnAliasedFieldsOverSnakeCase() throws IOException {

                record SanakeCaseField(int some_value, double someValue) {
                }

                var writerTest = new ParquetWriterTest<>(SanakeCaseField.class);
                writerTest.write(new SanakeCaseField(1, 2.0));

                record MixedValues(@Alias("some_value") int foo, @Alias("someValue") double bar) {
                }

                var reader = new CarpetReader<>(writerTest.getTestFile(), MixedValues.class)
                        .withFieldMatchingStrategy(SNAKE_CASE);
                MixedValues value = reader.iterator().next();
                assertEquals(new MixedValues(1, 2.0), value);
            }

        }

    }

    @Nested
    class Dictionary {

        @Nested
        class StringDictionary {

            @Test
            void useDictionaryString() throws IOException {
                Schema schema = schemaType("DictionaryString")
                        .optionalString("value")
                        .endRecord();

                int differentValues = 30;
                int totalRows = 10000;
                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    for (int i = 0; i < totalRows; i++) {
                        var it = 0;
                        for (int j = 0; j < differentValues; j++) {
                            Record record = new Record(schema);
                            record.put("value", "STR_" + Integer.toString(it++));
                            writer.write(record);
                        }
                    }
                });

                record StringRecord(String value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(StringRecord.class)) {
                    List<String> ref = new ArrayList<>();
                    for (int j = 0; j < differentValues; j++) {
                        ref.add(carpetReader.read().value);
                    }
                    for (int i = 0; i < totalRows - 1; i++) {
                        for (int j = 0; j < differentValues; j++) {
                            // Compare object references, not equals
                            assertTrue(ref.get(j) == carpetReader.read().value);
                        }
                    }
                }
            }

            @Test
            void useDictionaryStringCollection() throws IOException {

                int valuesPerRow = 25;
                int totalRows = 1000;

                var it = 0;
                List<String> vals = new ArrayList<>();
                for (int i = 0; i < valuesPerRow; i++) {
                    vals.add("STR_" + Integer.toString(it++));
                }

                record CollectionString(int id, List<String> values) {
                }

                List<CollectionString> rows = new ArrayList<>();
                for (int j = 0; j < totalRows; j++) {
                    rows.add(new CollectionString(j, vals));
                }

                var writerTest = new ParquetWriterTest<>(CollectionString.class);
                writerTest.write(rows);

                var reader = writerTest.getCarpetReader();
                List<String> ref = reader.read().values();
                for (int i = 1; i < totalRows; i++) {
                    CollectionString row = reader.read();
                    for (int j = 0; j < valuesPerRow; j++) {
                        // Compare object references, not equals
                        assertTrue(ref.get(j) == row.values().get(j));
                    }
                }
            }
        }

        @Nested
        class LocaldateDictionary {

            @Test
            void useDictionaryLocalDate() throws IOException {
                Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                Schema schema = SchemaBuilder.builder().record("LocalDateDictionary").fields()
                        .name("value").type(dateType).noDefault()
                        .endRecord();

                int differentValues = 30;
                int totalRows = 10000;
                GenericData genericDataModel = new GenericData();
                genericDataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
                var readerTest = new ParquetReaderTest(schema);
                readerTest.writerWithModel(genericDataModel, writer -> {
                    for (int i = 0; i < totalRows; i++) {
                        var firstDate = LocalDate.of(2022, 11, 21);
                        var it = firstDate;
                        for (int j = 0; j < differentValues; j++) {
                            Record record = new Record(schema);
                            record.put("value", it);
                            writer.write(record);
                            it = it.plusDays(1);
                        }
                    }
                });

                record LocalDateRecord(LocalDate value) {
                }

                try (var carpetReader = readerTest.getCarpetReader(LocalDateRecord.class)) {
                    List<LocalDate> ref = new ArrayList<>();
                    for (int j = 0; j < differentValues; j++) {
                        ref.add(carpetReader.read().value);
                    }
                    for (int i = 0; i < totalRows - 1; i++) {
                        for (int j = 0; j < differentValues; j++) {
                            // Compare object references, not equals
                            assertTrue(ref.get(j) == carpetReader.read().value);
                        }
                    }
                }
            }

            @Test
            void useDictionaryLocalDateCollection() throws IOException {

                int valuesPerRow = 25;
                int totalRows = 1000;

                var firstDate = LocalDate.of(2022, 11, 21);
                var it = firstDate;
                List<LocalDate> vals = new ArrayList<>();
                for (int i = 0; i < valuesPerRow; i++) {
                    vals.add(it);
                }

                record CollectionLocalDate(int id, List<LocalDate> dates) {
                }

                List<CollectionLocalDate> rows = new ArrayList<>();
                for (int j = 0; j < totalRows; j++) {
                    rows.add(new CollectionLocalDate(j, vals));
                }

                var writerTest = new ParquetWriterTest<>(CollectionLocalDate.class);
                writerTest.write(rows);

                var reader = writerTest.getCarpetReader();
                List<LocalDate> ref = reader.read().dates();
                for (int i = 1; i < totalRows; i++) {
                    CollectionLocalDate row = reader.read();
                    for (int j = 0; j < valuesPerRow; j++) {
                        // Compare object references, not equals
                        assertTrue(ref.get(j) == row.dates.get(j));
                    }
                }
            }
        }

    }

    private static FieldAssembler<Schema> schemaType(String type) {
        return SchemaBuilder.builder().record(type).fields();
    }

}
